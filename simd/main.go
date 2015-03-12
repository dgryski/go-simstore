package main

import (
	"bufio"
	"encoding/json"
	"expvar"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"

	"github.com/DataWraith/vptree"
	"github.com/dgryski/go-simstore"
	"github.com/dgryski/go-simstore/simhash"
)

var Metrics = struct {
	Requests   *expvar.Int
	Signatures *expvar.Int
}{
	Requests:   expvar.NewInt("requests"),
	Signatures: expvar.NewInt("signatures"),
}

type vpnode struct {
	h  uint64
	id uint64
}

func hamming(a, b interface{}) float64 { return float64(simhash.Distance(a.(vpnode).h, b.(vpnode).h)) }

func main() {

	port := flag.Int("p", 8080, "port to listen on")
	input := flag.String("f", "", "file with signatures to load")
	useVPTree := flag.Bool("vptree", true, "load only vptree")
	useStore := flag.Bool("store", true, "load the simstore")

	flag.Parse()

	log.Println("starting simd")

	var store *simstore.Store
	if *useStore {
		store = &simstore.Store{}
	}

	var vpt *vptree.VPTree

	f, err := os.Open(*input)
	if err != nil {
		log.Fatalf("unable to load %q: %v", *input, err)
	}

	scanner := bufio.NewScanner(f)
	var items []interface{}
	var lines int
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

		if *useVPTree {
			items = append(items, vpnode{sig, uint64(id)})
		}
		if *useStore {
			store.Add(sig, uint64(id))
		}
		lines++

		if lines%(1<<20) == 0 {
			log.Println("processed", lines)
		}
	}

	log.Println("loaded", lines)
	Metrics.Signatures.Set(int64(lines))
	if *useStore {
		store.Finish()
		log.Println("simstore done")
		http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) { searchHandler(w, r, store) })
	}

	if *useVPTree {
		vpt = vptree.New(hamming, items)
		log.Println("vptree done")
		http.HandleFunc("/topk", func(w http.ResponseWriter, r *http.Request) { topkHandler(w, r, vpt) })
	}

	log.Println("listening on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

func topkHandler(w http.ResponseWriter, r *http.Request, vpt *vptree.VPTree) {

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

	matches, distances := vpt.Search(vpnode{sig64, 0}, k)

	type hit struct {
		ID uint64  `json:"id"`
		D  float64 `json:"d"`
	}

	var results []hit

	for i, m := range matches {
		results = append(results, hit{ID: m.(vpnode).id, D: distances[i]})
	}

	json.NewEncoder(w).Encode(results)
}

func searchHandler(w http.ResponseWriter, r *http.Request, store *simstore.Store) {

	Metrics.Requests.Add(1)

	sigstr := r.FormValue("sig")

	var sig64 uint64

	var err error
	sig64, err = strconv.ParseUint(sigstr, 16, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	matches := store.Find(sig64)

	json.NewEncoder(w).Encode(matches)
}
