package simstore

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/dgryski/go-huff"
)

func TestCompress(t *testing.T) {

	const signatures = 1 << 25

	u := make(u64store, signatures)
	for i := range u {
		u[i] = uint64(rand.Int63())
	}
	sort.Sort(u)

	var b bytes.Buffer
	compress(u, &b)
	buffer := b.Bytes()

	sz := len(u) * int(unsafe.Sizeof(u[0]))
	csz := len(buffer)
	t.Logf("entries=%d size=%d compressed=%d savings=%d%%\n", signatures, sz, csz, int(100-100*float64(csz)/float64(sz)))

	var d u64store
	var err error
	var offset int

	blen := len(buffer)

	codebook, buffer := buffer[blen-66:], buffer[:blen-66]

	hd, err := huff.NewDecoder(codebook)
	if err != nil {
		t.Fatalf("error creating decoder")
	}

	var totalDuration time.Duration
	var blocks int

	for i := range u {
		if len(d) == 0 {
			//	t.Logf("loading compressed block at offset %d\n", offset)

			t0 := time.Now()
			d, err = decompressBlock(hd, bytes.NewReader(buffer[offset:]))
			totalDuration += time.Since(t0)
			blocks++
			if err != nil {
				t.Errorf("decompress err = %+v\n", err)
			}
			offset += 1024
		}

		if u[i] != d[0] {
			t.Fatalf("d[%d]=%x, want %x\n", i, d[0], u[i])
		}
		d = d[1:]
	}

	t.Logf("blocks=%d, average decompression time %v", blocks, totalDuration/time.Duration(blocks))

}
