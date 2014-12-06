// Package simstore implements a storage layer for simhash locality-sensitive hashes.
package simstore

import (
	"sort"

	"github.com/dgryski/go-simstore/simhash"
)

type entry struct {
	hash  uint64
	docid uint64
}

type table []entry

// TODO(dgryski): table persistent (boltdb?)
// TODO(dgryski): replace array with btree?
// TODO(dgryski): split hashes and docid into different arrays to optimize cache usage

func (t table) Len() int           { return len(t) }
func (t table) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t table) Less(i, j int) bool { return t[i].hash < t[j].hash }

func (t table) find(sig uint64) []uint64 {

	const mask = 0xfffffff000000000

	prefix := sig & mask
	// TODO(dgryski): interpolation search instead of binary search
	i := sort.Search(len(t), func(i int) bool { return t[i].hash >= prefix })

	var ids []uint64

	for i < len(t) && t[i].hash&mask == prefix {
		if simhash.Distance(t[i].hash, sig) <= 3 {
			ids = append(ids, t[i].docid)
		}
		i++
	}

	return ids
}

type Store struct {
	tables [16]table
}

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

func (s *Store) Finish() {
	for i := range s.tables {
		sort.Sort(s.tables[i])
	}
}

func (s *Store) Find(sig uint64) []uint64 {

	var ids []uint64

	// TODO(dgryski): search in parallel
	for i := range s.tables {
		ids = append(ids, s.tables[i].find(sig)...)
	}

	return ids
}
