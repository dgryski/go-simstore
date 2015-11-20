package simstore

import (
	"testing"
	"testing/quick"
)

func TestUnshuffle(t *testing.T) {

	f := func(hash uint64) bool {
		s := New3(1)
		s.Add(hash, 0)

		for i := range s.tables {
			if got := s.unshuffle(s.tables[i][0].hash, i); got != hash {
				t.Errorf("unshuffle(tables[%d])=%016x, want %016x\n", i, got, hash)
				return false
			}
		}
		return true
	}

	quick.Check(f, nil)
}

func TestUnshuffle6(t *testing.T) {

	f := func(hash uint64) bool {
		s := New6(1)
		s.Add(hash, 0)

		for i := range s.tables {
			if got := s.unshuffle(s.tables[i][0].hash, i); got != hash {
				t.Errorf("unshuffle(tables[%d])=%016x, want %016x\n", i, got, hash)
				return false
			}
		}
		return true
	}

	quick.Check(f, nil)
}
