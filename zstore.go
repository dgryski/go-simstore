package simstore

import (
	"bytes"
	"errors"
	"io"
	"sort"

	"github.com/dgryski/go-bits"
	"github.com/dgryski/go-bitstream"
	"github.com/dgryski/go-huff"
)

const (
	blockSize     = 1024
	blockSizeBits = blockSize * 8
)

type zstore struct {
	index []uint64
	d     *huff.Decoder
	b     []byte
	u     u64slice
}

func NewZStore(hashes int) u64store {
	return &zstore{u: make(u64slice, 0, hashes)}
}

func (z *zstore) add(p uint64) {
	z.u = append(z.u, p)
}

func (z *zstore) finish() {
	z.u.finish()
	z.compress()
	z.u = nil
}

func (z *zstore) blocks() int {
	return len(z.index)
}

func (z *zstore) compress() {

	var counts [64]int

	for i := 1; i < len(z.u); i++ {
		lz := bits.Clz(z.u[i] ^ z.u[i-1])
		if lz == 64 {
			// duplicate signature, ignore
			continue
		}
		counts[lz]++
	}

	e := huff.NewEncoder(counts[:])

	var w bytes.Buffer
	hw := e.Writer(&w)

	eofbits := e.SymbolLen(huff.EOF)

	var nbits int

	z.index = append(z.index, z.u[0])
	hw.WriteBits(z.u[0], 64)
	nbits += 64

	for i := 1; i < len(z.u); i++ {
		// how much space required to compress this hash?
		lz := int(bits.Clz(z.u[i] ^ z.u[i-1]))
		if lz == 64 {
			// duplicate signature -- ignore
			continue
		}
		hlen := e.SymbolLen(uint32(lz))
		rest := 64 - lz - 1

		// fits in this block
		if nbits+hlen+rest+eofbits < blockSizeBits {
			hw.WriteSymbol(uint32(lz))
			nbits += hlen
			hw.WriteBits(z.u[i], rest)
			nbits += rest
		} else if nbits+eofbits < blockSizeBits {
			// doesn't fit, there should always be space for EOF
			hw.WriteSymbol(huff.EOF)
			nbits += eofbits

			for nbits < blockSizeBits && nbits%8 != 0 {
				hw.WriteBit(bitstream.Zero)
				nbits++
			}

			for nbits < blockSizeBits {
				hw.WriteByte(0)
				nbits += 8
			}

			nbits = 0

			// this block is done, start the next block
			h := z.u[i]
			z.index = append(z.index, h)
			hw.WriteBits(h, 64)
			nbits += 64
		} else {
			panic("block overflow")
		}
	}

	hw.WriteSymbol(huff.EOF)
	hw.Flush(bitstream.Zero)

	z.d = e.Decoder()
	z.b = w.Bytes()
}

var (
	ErrCorruptFile  = errors.New("zstore: corrupt file")
	ErrInvalidBlock = errors.New("zstore: invalid block")
)

func (z zstore) decompressBlock(block int) (u64slice, error) {

	if block < 0 || block >= len(z.index) {
		return nil, ErrInvalidBlock
	}

	offs := block * 1024
	end := offs + 1024
	if end > len(z.b) {
		end = len(z.b)
	}

	br := bitstream.NewReader(bytes.NewReader(z.b[offs:end]))

	sig, err := br.ReadBits(64)
	if err != nil {
		return nil, err
	}

	var u u64slice
	u = append(u, sig)

	prev := sig
	for {
		samebits, err := z.d.ReadSymbol(br)
		if samebits == huff.EOF {
			break
		}
		diffbits, err := br.ReadBits(int(64 - samebits - 1))
		if err != nil {
			return nil, ErrCorruptFile
		}

		mask := uint64(((1 << samebits) - 1) << (64 - samebits))
		sig = (prev & mask) | (1 << (64 - samebits - 1)) | diffbits

		u = append(u, sig)
		prev = sig
		if err == io.EOF {
			break
		}
	}

	return u, nil
}

func (z *zstore) find(sig, mask uint64, d int) []uint64 {

	prefix := sig & mask
	// TODO(dgryski): interpolation search instead of binary search; 2x speed up vs. sort.Search()
	block := sort.Search(len(z.index), func(i int) bool { return z.index[i] >= prefix })

	var ids []uint64

	if block > 0 {
		if u, err := z.decompressBlock(block - 1); err == nil {
			ids = append(ids, u.find(sig, mask, d)...)
		}
	}

	for block < z.blocks() && z.index[block]&mask == prefix {
		if u, err := z.decompressBlock(block); err == nil {
			ids = append(ids, u.find(sig, mask, d)...)
		}
		block++
	}
	return ids
}
