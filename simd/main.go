package main

import (
	"bufio"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/dgryski/go-simstore"
	"github.com/dgryski/go-simstore/vptree"
)

var Metrics = struct {
	Requests   *expvar.Int
	Signatures *expvar.Int
}{
	Requests:   expvar.NewInt("requests"),
	Signatures: expvar.NewInt("signatures"),
}

var BuildVersion string = "(development build)"

type Config struct {
	store  simstore.Storage
	vptree *vptree.VPTree
}

var config unsafe.Pointer // actual type is *Config
// CurrentConfig atomically returns the current configuration
func CurrentConfig() *Config { return (*Config)(atomic.LoadPointer(&config)) }

// UpdateConfig atomically swaps the current configuration
func UpdateConfig(cfg *Config) { atomic.StorePointer(&config, unsafe.Pointer(cfg)) }

func main() {

	port := flag.Int("p", 8080, "port to listen on")
	input := flag.String("f", "", "file with signatures to load")
	useVPTree := flag.Bool("vptree", true, "load vptree")
	useStore := flag.Bool("store", true, "load simstore")
	storeSize := flag.Int("size", 6, "simstore size (3/6)")
	storeSigs := flag.Int("sigs", 32e6, "expected number of signatures for preallocation")
	cpus := flag.Int("cpus", runtime.NumCPU(), "value of GOMAXPROCS")
	myNumber := flag.Uint("no", 0, "id of this machine")
	totalMachines := flag.Uint("of", 1, "number of machines to distribute the table among")
	small := flag.Bool("small", false, "use small memory for size 3")

	flag.Parse()

	expvar.NewString("BuildVersion").Set(BuildVersion)

	log.Println("starting simd", BuildVersion)

	log.Println("setting GOMAXPROCS=", *cpus)
	runtime.GOMAXPROCS(*cpus)

	if *input == "" {
		log.Fatalln("no import hash list provided (-f)")
	}

	err := loadConfig(*input, *useStore, *storeSize, *small, *storeSigs, *useVPTree, *myNumber, *totalMachines)
	if err != nil {
		log.Fatalln("unable to load config:", err)
	}

	if *useStore {
		http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) { searchHandler(w, r) })
	}

	if *useVPTree {
		http.HandleFunc("/topk", func(w http.ResponseWriter, r *http.Request) { topkHandler(w, r) })
	}

	go func() {
		sigs := make(chan os.Signal)
		signal.Notify(sigs, syscall.SIGHUP)

		for {
			select {
			case <-sigs:
				log.Println("caught SIGHUP, reloading")

				err := loadConfig(*input, *useStore, *storeSize, *small, *storeSigs, *useVPTree, *myNumber, *totalMachines)
				if err != nil {
					log.Println("reload failed: ignoring:", err)
					break
				}
			}
		}

	}()

	log.Println("listening on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

func loadConfig(input string, useStore bool, storeSize int, small bool, storeSigs int, useVPTree bool, myNumber uint, totalMachines uint) error {
	var store simstore.Storage
	if useStore {
		switch storeSize {
		case 3:
			if small {
				store = simstore.New3Small(storeSigs)
			} else {
				store = simstore.New3(storeSigs)
			}
		case 6:
			store = simstore.New6(storeSigs)
		default:
			return fmt.Errorf("unknown storage size: %d", storeSize)
		}

		log.Println("using simstore size", storeSize)
	}

	var vpt *vptree.VPTree

	f, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("unable to load %q: %v", input, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var items []vptree.Item
	var lines int
	var signatures int
	for scanner.Scan() {

		fields := strings.Fields(scanner.Text())

		id, err := strconv.Atoi(fields[0])
		if err != nil {
			log.Printf("%d: error parsing id: %v", lines, err)
			continue
		}

		sig, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			log.Printf("%d: error parsing signature: %v", lines, err)
			continue
		}

		if uint(sig)%totalMachines == myNumber {
			if useVPTree {
				items = append(items, vptree.Item{sig, uint64(id)})
			}
			if useStore {
				store.Add(sig, uint64(id))
			}
			signatures++
		}
		lines++

		if lines%(1<<20) == 0 {
			log.Println("processed", lines)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("error during scan: ", err)
	}

	log.Println("loaded", lines)
	Metrics.Signatures.Set(int64(signatures))
	if useStore {
		store.Finish()
		log.Println("simstore done")
	}

	if useVPTree {
		vpt = vptree.New(items)
		log.Println("vptree done")
	}

	UpdateConfig(&Config{store: store, vptree: vpt})
	return nil
}

func topkHandler(w http.ResponseWriter, r *http.Request) {

	Metrics.Requests.Add(1)

	sigstr := r.FormValue("sig")
	sig64, err := strconv.ParseUint(sigstr, 16, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kstr := r.FormValue("k")
	if kstr == "" {
		kstr = "10"
	}

	k, err := strconv.Atoi(kstr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	vpt := CurrentConfig().vptree

	matches, distances := vpt.Search(sig64, k)

	type hit struct {
		ID uint64  `json:"id"`
		D  float64 `json:"d"`
	}

	var results []hit

	for i, m := range matches {
		results = append(results, hit{ID: m.ID, D: distances[i]})
	}

	json.NewEncoder(w).Encode(results)
}

func searchHandler(w http.ResponseWriter, r *http.Request) {

	Metrics.Requests.Add(1)

	sigstr := r.FormValue("sig")

	var sig64 uint64

	var err error
	sig64, err = strconv.ParseUint(sigstr, 16, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	store := CurrentConfig().store

	matches := store.Find(sig64)

	json.NewEncoder(w).Encode(matches)
}
