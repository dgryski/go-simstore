package simstore

import (
	"testing"
	"testing/quick"
)

func TestUnshuffle(t *testing.T) {

	f := func(hash uint64) bool {
		s := New3(1, NewU64Slice)
		s.Add(hash, 0)

		for i := range s.rhashes {
			if got := s.unshuffle((*s.rhashes[i].(*u64slice))[0], i); got != hash {
				t.Errorf("unshuffle(rhashes[%d])=%016x, want %016x\n", i, got, hash)
				return false
			}
		}
		return true
	}

	quick.Check(f, nil)
}

func TestUnshuffle6(t *testing.T) {

	f := func(hash uint64) bool {
		s := New6(1, NewU64Slice)
		s.Add(hash, 0)

		for i := range s.rhashes {
			if got := s.unshuffle((*s.rhashes[i].(*u64slice))[0], i); got != hash {
				t.Errorf("unshuffle(rhashes[%d])=%016x, want %016x\n", i, got, hash)
				return false
			}
		}
		return true
	}

	quick.Check(f, nil)
}
