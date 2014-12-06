package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/dgryski/go-simstore"
	"github.com/dgryski/go-simstore/simhash"
)

func main() {

	port := flag.Int("p", 8080, "port to listen on")
	input := flag.String("f", "", "file with filenames to load")

	flag.Parse()

	var docs []string
	var store simstore.Store

	f, err := os.Open(*input)
	if err != nil {
		log.Fatalf("unable to load %q: %v", *input, err)
	}

	scanner := bufio.NewScanner(f)
	var id uint64
	for scanner.Scan() {

		fname := scanner.Text()

		sig, err := hashFile(fname)
		if err != nil {
			log.Printf("%s %+v\n", fname, err)
			continue
		}

		store.Add(sig, id)
		id++
		docs = append(docs, fname)
	}

	store.Finish()

	log.Println("fingerprinted", id)

	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) { searchHandler(w, r, docs, &store) })
	log.Println("listening on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

func searchHandler(w http.ResponseWriter, r *http.Request, docs []string, store *simstore.Store) {

	sigstr := r.FormValue("sig")

	var sig64 uint64

	if sigstr == "" {
		f := r.FormValue("file")
		var err error
		sig64, err = hashFile(f)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		var err error
		sig64, err = strconv.ParseUint(sigstr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	matches := store.Find(sig64)

	var fnames []string

	for _, m := range matches {
		fnames = append(fnames, docs[m])
	}
	json.NewEncoder(w).Encode(fnames)
}

func hashFile(fname string) (uint64, error) {
	f, err := os.Open(fname)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fscan := bufio.NewScanner(f)
	fscan.Split(bufio.ScanWords)

	sig := simhash.Hash(fscan)
	log.Printf("%016x %s", sig, fname)

	return sig, nil
}
