package simstore

import (
	"bytes"
	"errors"
	"io"

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
}

func (z zstore) blocks() int {
	return len(z.index)
}

func compress(u u64store) zstore {

	var counts [64]int

	for i := 1; i < len(u); i++ {
		lz := bits.Clz(u[i] ^ u[i-1])
		counts[lz]++
	}

	e := huff.NewEncoder(counts[:])

	var w bytes.Buffer
	hw := e.Writer(&w)

	eofbits := e.SymbolLen(huff.EOF)

	var nbits int
	var index []uint64

	index = append(index, u[0])
	hw.WriteBits(u[0], 64)
	nbits += 64

	for i := 1; i < len(u); i++ {

		// how much space required to compress this hash?
		xor := u[i] ^ u[i-1]
		lz := int(bits.Clz(xor))
		hlen := e.SymbolLen(uint32(lz))
		rest := 64 - lz - 1

		// fits in this block
		if nbits+hlen+rest+eofbits < blockSizeBits {
			hw.WriteSymbol(uint32(lz))
			nbits += hlen
			hw.WriteBits(u[i], rest)
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
			h := u[i]
			index = append(index, h)
			hw.WriteBits(h, 64)
			nbits += 64
		} else {
			panic("block overflow")
		}
	}

	hw.WriteSymbol(huff.EOF)
	hw.Flush(bitstream.Zero)

	return zstore{index, e.Decoder(), w.Bytes()}
}

var (
	ErrCorruptFile  = errors.New("zstore: corrupt file")
	ErrInvalidBlock = errors.New("zstore: invalid block")
)

func (z zstore) decompressBlock(block int) (u64store, error) {

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

	var u u64store
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
