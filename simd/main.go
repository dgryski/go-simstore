package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"time"
	"io"
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
	"github.com/peterbourgon/g2g"
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
	cpus := flag.Int("cpus", runtime.NumCPU(), "value of GOMAXPROCS")
	myNumber := flag.Int("no", 0, "id of this machine")
	totalMachines := flag.Int("of", 1, "number of machines to distribute the table among")
	small := flag.Bool("small", false, "use small memory for size 3")
	compressed := flag.Bool("z", false, "use compressed tables")
	graphite_host := flag.String("graphite_host", "", "graphite host")
	graphite_port := flag.String("graphite_port", "", "graphite port")
	graphite_namespace := flag.String("graphite_namespace", "", "graphite namespace")

	flag.Parse()

	expvar.NewString("BuildVersion").Set(BuildVersion)

	log.Println("starting simd", BuildVersion)

	log.Println("setting GOMAXPROCS=", *cpus)
	runtime.GOMAXPROCS(*cpus)

	if *input == "" {
		log.Fatalln("no import hash list provided (-f)")
	}
	if *graphite_host != "" && *graphite_namespace == "" {
		log.Fatalln("no graphite namespace provided (-f)")	
	}

	err := loadConfig(*input, *useStore, *storeSize, *small, *compressed, *useVPTree, *myNumber, *totalMachines)
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

		for range sigs {
			log.Println("caught SIGHUP, reloading")

			err := loadConfig(*input, *useStore, *storeSize, *small, *compressed, *useVPTree, *myNumber, *totalMachines)
			if err != nil {
				log.Println("reload failed: ignoring:", err)
				break
			}
		}
	}()

	if *graphite_host != "" {
		if *graphite_port == "" {
        	*graphite_port = "2003"
         }
         hostport := *graphite_host + ":" + *graphite_port
         graphite := g2g.NewGraphite(hostport, 60*time.Second, 5*time.Second)
         hostname, _ := os.Hostname()
         namespace := *graphite_namespace + "." + hostname + ".metrics"
         graphite.Register(namespace + ".signatures", Metrics.Signatures)
		 graphite.Register(namespace + ".requests", Metrics.Requests)
	}

	log.Println("listening on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

// https://stackoverflow.com/questions/24562942/golang-how-do-i-determine-the-number-of-lines-in-a-file-efficiently
func lineCounter(input string) (int, error) {
	r, err := os.Open(input)
	if err != nil {
		return 0, fmt.Errorf("unable to load %q: %v", input, err)
	}
	defer r.Close()

	buf := make([]byte, 8196)
	var count int
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}

func loadConfig(input string, useStore bool, storeSize int, small bool, compressed bool, useVPTree bool, myNumber int, totalMachines int) error {
	var store simstore.Storage

	totalLines, err := lineCounter(input)
	if err != nil {
		return fmt.Errorf("unable to load %q: %v", input, err)
	}

	var sigsEstimate = totalLines

	log.Printf("totalLines=%+v\n", totalLines)

	if totalMachines != 1 {
		// estimate how many signatures will land on this machine, plus a fudge
		sigsEstimate = totalLines / totalMachines
		sigsEstimate += int(float64(sigsEstimate) * 0.05)
	}

	log.Printf("preallocating for %d estimated signatures\n", sigsEstimate)

	factory := simstore.NewU64Slice
	if compressed {
		factory = simstore.NewZStore
	}

	if useStore {
		switch storeSize {
		case 3:
			if small {
				store = simstore.New3Small(sigsEstimate)
			} else {
				store = simstore.New3(sigsEstimate, factory)
			}
		case 6:
			store = simstore.New6(sigsEstimate, factory)
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

		if sig%uint64(totalMachines) == uint64(myNumber) {
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
			log.Printf("processed %d of %d", lines, totalLines)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("error during scan: ", err)
	}

	log.Printf("loaded %d lines, %d signatues (%f%% of estimated)", lines, signatures, 100*float64(signatures)/float64(sigsEstimate))
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
