package simstore

import (
	"math/rand"
	"testing"
)

const size = 1000000
const queries = 1000000

func TestAdd3(t *testing.T) {
	s := New3(size, NewU64Slice)
	testAdd(t, s, size, queries, 3)
}

func TestAdd3Small(t *testing.T) {
	s := New3Small(size)
	testAdd(t, s, size, queries, 3)
}

func TestAdd6(t *testing.T) {
	s := New6(size, NewU64Slice)
	testAdd(t, s, size, queries, 6)
}

func TestAdd3Z(t *testing.T) {
	s := New3(size, NewZStore)
	testAdd(t, s, size, queries/100, 3)
}

func TestAdd6Z(t *testing.T) {
	s := New6(size, NewZStore)
	testAdd(t, s, size, queries/100, 6)
}

func testAdd(t *testing.T, s Storage, size, queries, d int) {

	rand.Seed(0)

	for i := 0; i < size; i++ {
		s.Add(uint64(rand.Int63()), uint64(i))
	}

	sig := uint64(0x001122334455667788)
	s.Add(sig, 0xdeadbeef)

	s.Finish()

	var fails int

	for j := 0; j < queries; j++ {

		q := sig

		// bits := rand.Intn(7)
		bits := d

		for i := 0; i < bits; i++ {
			q ^= 1 << uint(rand.Intn(64))
		}

		found := s.Find(q)
		var foundbeef bool
		for _, v := range found {
			if v == 0xdeadbeef {
				foundbeef = true
				break
			}

		}
		if !foundbeef {
			t.Errorf("sig = %016x (%064b) (found=%v)\n", sig, sig^q, found)
			fails++
		}
	}

	if fails != 0 {
		t.Logf("fails = %f", 100*float64(fails)/float64(queries))
	}
}
