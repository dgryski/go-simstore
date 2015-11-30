package simstore

import (
	"math/rand"
	"sort"
	"testing"
	"time"
	"unsafe"
)

func TestCompress(t *testing.T) {

	const signatures = 1 << 20

	var z zstore

	u := make(u64slice, signatures)
	for i := range u {
		u[i] = uint64(rand.Int63())
		z.add(u[i])
	}
	sort.Sort(u)

	z.finish()

	sz := len(u) * int(unsafe.Sizeof(u[0]))
	csz := len(z.b)
	t.Logf("entries=%d size=%d compressed=%d savings=%d%%\n", signatures, sz, csz, int(100-100*float64(csz)/float64(sz)))

	var d u64slice
	var err error

	var totalDuration time.Duration
	var blocks int

	for i := range u {
		if len(d) == 0 {
			t0 := time.Now()
			d, err = z.decompressBlock(blocks)
			totalDuration += time.Since(t0)
			blocks++
			if err != nil {
				t.Errorf("decompress err = %+v\n", err)
			}
		}

		if u[i] != d[0] {
			t.Fatalf("d[%d]=%x, want %x\n", i, d[0], u[i])
		}
		d = d[1:]
	}

	t.Logf("blocks=%d, average decompression time %v", blocks, totalDuration/time.Duration(blocks))

}

func TestDuplicateSignatures(t *testing.T) {

	const signatures = 20

	var z zstore

	u := make(u64slice, signatures)
	for i := range u {
		u[i] = uint64(rand.Int63())
		z.add(u[i])
		z.add(u[i])
	}
	sort.Sort(u)

	z.finish()

	d, err := z.decompressBlock(0)
	if err != nil {
		t.Fatalf("error during decompressBlock: %v\n", err)
	}
	if len(d) != len(u) {
		t.Fatalf("len(d)=%d len(u)=%d\n", len(d), len(u))
	}

	for i := range d {
		if d[i] != u[i] {
			t.Errorf("d[%d]=%x u[%d]=%x\n", i, d[i], i, u[i])
		}
	}
}
