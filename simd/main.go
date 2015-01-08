package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/dgryski/go-simstore"
)

func main() {

	port := flag.Int("p", 8080, "port to listen on")
	input := flag.String("f", "", "file with filenames to load")

	flag.Parse()

	log.Println("starting simd")

	var store simstore.Store

	f, err := os.Open(*input)
	if err != nil {
		log.Fatalf("unable to load %q: %v", *input, err)
	}

	scanner := bufio.NewScanner(f)
	var lines uint64
	for scanner.Scan() {

		fields := strings.Fields(scanner.Text())

		id, err := strconv.Atoi(fields[0])
		if err != nil {
			log.Printf("%d: %q: %v", lines, fields[0], err)
			continue
		}

		sig, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			log.Printf("%d: %q: %v", lines, fields[1], err)
			continue
		}

		store.Add(sig, uint64(id))
		lines++

		if lines%(1<<20) == 0 {
			log.Println("processed", lines)
		}
	}

	log.Println("loaded", lines)
	store.Finish()
	log.Println("done")

	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) { searchHandler(w, r, &store) })
	log.Println("listening on port", *port)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

func searchHandler(w http.ResponseWriter, r *http.Request, store *simstore.Store) {

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
