// Package simstore implements a storage layer for simhash locality-sensitive hashes.
/*

This package is an implementation of section 3 of "Detecting Near-Duplicates
for Web Crawling" by Manku, Jain, and Sarma,

    http://www2007.org/papers/paper215.pdf

It is hard-coded for hamming distance 3 or 6.
*/
package simstore

import (
	"runtime"
	"sort"
	"sync"
)

// TODO(dgryski): split hashes and docid into different arrays to optimize cache usage

type entry struct {
	hash  uint64
	docid uint64
}

// TODO(dgryski): table persistent via mmap
type table []entry

func (t table) Len() int           { return len(t) }
func (t table) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t table) Less(i, j int) bool { return t[i].hash < t[j].hash }

const mask3 = 0xfffffff000000000

func (t table) find(sig, mask uint64, d int) []uint64 {

	prefix := sig & mask
	// TODO(dgryski): interpolation search instead of binary search
	i := sort.Search(len(t), func(i int) bool { return t[i].hash >= prefix })

	if i == -1 {
		return nil
	}

	var ids []uint64

	for i < len(t) && t[i].hash&mask == prefix {
		if distance(t[i].hash, sig) <= d {
			ids = append(ids, t[i].docid)
		}
		i++
	}

	return ids
}

// Store is a storage engine for 64-bit hashes
type Store struct {
	tables []table
}

// New3 returns a Store for searching hamming distance <= 3
func New3() *Store {
	s := Store{}
	s.tables = make([]table, 16)
	return &s
}

// Add inserts a signature and document id into the store
func (s *Store) Add(sig uint64, docid uint64) {

	var t int
	for i := 0; i < 4; i++ {
		p := sig
		s.tables[t] = append(s.tables[t], entry{hash: p, docid: docid})
		t++
		p = (sig & 0xffff000000ffffff) | (sig & 0x0000fff000000000 >> 12) | (sig & 0x0000000fff000000 << 12)
		s.tables[t] = append(s.tables[t], entry{hash: p, docid: docid})
		t++

		p = (sig & 0xffff000fff000fff) | (sig & 0x0000fff000000000 >> 24) | (sig & 0x0000000000fff000 << 24)
		s.tables[t] = append(s.tables[t], entry{hash: p, docid: docid})
		t++

		p = (sig & 0xffff000ffffff000) | (sig & 0x0000fff000000000 >> 36) | (sig & 0x0000000000000fff << 36)
		s.tables[t] = append(s.tables[t], entry{hash: p, docid: docid})
		t++

		sig = (sig << 16) | (sig >> (64 - 16))
	}
}

type limiter chan struct{}

func (l limiter) enter() { l <- struct{}{} }
func (l limiter) leave() { <-l }

// Finish prepares the store for searching.  This must be called once after all
// the signatures have been added via Add().
func (s *Store) Finish() {

	l := make(limiter, runtime.GOMAXPROCS(0))

	var wg sync.WaitGroup
	for i := range s.tables {
		l.enter()
		wg.Add(1)
		go func(i int) {
			sort.Sort(s.tables[i])
			l.leave()
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// Find searches the store for all hashes hamming distance 3 or less from the
// query signature.  It returns the associated list of document ids.
func (s *Store) Find(sig uint64) []uint64 {

	var ids []uint64

	// TODO(dgryski): search in parallel
	var t int
	for i := 0; i < 4; i++ {
		p := sig
		ids = append(ids, s.tables[t].find(p, mask3, 3)...)
		t++

		p = (sig & 0xffff000000ffffff) | (sig & 0x0000fff000000000 >> 12) | (sig & 0x0000000fff000000 << 12)
		ids = append(ids, s.tables[t].find(p, mask3, 3)...)
		t++

		p = (sig & 0xffff000fff000fff) | (sig & 0x0000fff000000000 >> 24) | (sig & 0x0000000000fff000 << 24)
		ids = append(ids, s.tables[t].find(p, mask3, 3)...)
		t++

		p = (sig & 0xffff000ffffff000) | (sig & 0x0000fff000000000 >> 36) | (sig & 0x0000000000000fff << 36)
		ids = append(ids, s.tables[t].find(p, mask3, 3)...)
		t++

		sig = (sig << 16) | (sig >> (64 - 16))
	}

	return unique(ids)
}

func unique(ids []uint64) []uint64 {
	// dedup ids
	uniq := make(map[uint64]struct{})
	for _, id := range ids {
		uniq[id] = struct{}{}
	}

	ids = ids[:0]
	for k := range uniq {
		ids = append(ids, k)
	}

	return ids

}

// distance returns the hamming distance between v1 and v2
func distance(v1 uint64, v2 uint64) int {

	x := v1 ^ v2

	// bit population count, see
	// http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return int(x >> 56)
}
