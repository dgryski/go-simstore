/*
Package simhash implements the simhash document similarity hashing function.

http://www.cs.princeton.edu/courses/archive/spr04/cos598B/bib/CharikarEstim.pdf
http://infolab.stanford.edu/~manku/papers/07www-duplicates.pdf
http://irl.cse.tamu.edu/people/sadhan/papers/cikm2011.pdf
*/
package simhash

import "github.com/dchest/siphash"

// Hash returns a simhash value for the document returned by the scanner
func Hash(scanner FeatureScanner) uint64 {
	var signs [64]int64

	for scanner.Scan() {
		b := scanner.Bytes()
		h := siphash.Hash(0, 0, b)

		for i := 0; i < 64; i++ {
			negate := int(h) & 1
			// if negate is 1, we will negate '-1', below
			r := (-1 ^ -negate) + negate
			signs[i] += int64(r)
			h >>= 1
		}
	}

	var shash uint64

	// TODO: can probably be done with SSE?
	for i := 63; i >= 0; i-- {
		shash <<= 1
		shash |= uint64(signs[i]>>63) & 1
	}

	return shash
}

func Distance(v1 uint64, v2 uint64) int {

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
