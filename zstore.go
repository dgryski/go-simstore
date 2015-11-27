package simstore

import (
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
}

func compress(u u64store, w io.Writer) zstore {
	var counts [64]int

	for i := 1; i < len(u); i++ {
		lz := bits.Clz(u[i] ^ u[i-1])
		counts[lz]++
	}

	e := huff.NewEncoder(counts[:])

	hw := e.Writer(w)

	eofbits := e.SymbolLen(huff.EOF)

	var nbits int
	var index []uint64

	for i := range u {

		// start of a block
		if nbits == 0 {
			h := u[i]
			index = append(index, h)
			hw.WriteBits(h, 64)
			nbits += 64
			continue
		}

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

			// write the first part of the next block
			hw.WriteBits(u[i], 64)
			nbits += 64

		} else {
			panic("block overflow")
		}
	}

	hw.WriteSymbol(huff.EOF)
	hw.Flush(bitstream.Zero)

	// stuff the dictionary at the end
	w.Write(e.CodebookBytes())

	return zstore{index, e.Decoder()}
}

var ErrCorruptFile = errors.New("disktable: corrupt file")

func decompressBlock(hr *huff.Decoder, r io.Reader) (u64store, error) {
	var u u64store

	br := bitstream.NewReader(r)

	sig, err := br.ReadBits(64)
	if err != nil {
		return u, err
	}

	u = append(u, sig)

	prev := sig
	for {
		samebits, err := hr.ReadSymbol(br)
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
