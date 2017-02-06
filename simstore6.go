package simstore

type Storage interface {
	Add(sig, docid uint64)
	Find(sig uint64) []uint64
	Finish()
}

type Store6 struct {
	Store
}

func New6(hashes int, newStore func(hashes int) u64store) *Store6 {
	var s Store6
	s.rhashes = make([]u64store, 49)

	if hashes != 0 {
		s.docids = make(table, 0, hashes)
		for i := range s.rhashes {
			s.rhashes[i] = newStore(hashes)
		}
	}

	return &s
}

type permutation struct {
	sig, mask uint64
	number    int
}

func (s *Store6) generate_permutations(sig uint64) []permutation {
	var p uint64
	result := make([]permutation, 49)
	t := 0

	for i := 0; i < 6; i++ {
		p = sig
		result[t] = permutation{sig: p, mask: mask6_9_8, number: t}
		t++
		p = (sig & 0xff80007fffffffff) | (sig & 0x007f800000000000 >> 8) | (sig & 0x00007f8000000000 << 8)
		result[t] = permutation{sig: p, mask: mask6_9_8, number: t}
		t++
		p = (sig & 0xff807f807fffffff) | (sig & 0x007f800000000000 >> 16) | (sig & 0x0000007f80000000 << 16)
		result[t] = permutation{sig: p, mask: mask6_9_8, number: t}
		t++
		p = (sig & 0xff807fff807fffff) | (sig & 0x007f800000000000 >> 24) | (sig & 0x000000007f800000 << 24)
		result[t] = permutation{sig: p, mask: mask6_9_8, number: t}
		t++
		p = (sig & 0xff807fffff807fff) | (sig & 0x007f800000000000 >> 32) | (sig & 0x00000000007f8000 << 32)
		result[t] = permutation{sig: p, mask: mask6_9_8, number: t}
		t++
		p = (sig & 0xff807fffffff807f) | (sig & 0x007f800000000000 >> 40) | (sig & 0x0000000000007f80 << 40)
		result[t] = permutation{sig: p, mask: mask6_9_8, number: t}
		t++
		p = (sig & 0xff80ffffffffff80) | (sig & 0x007f000000000000 >> 48) | (sig & 0x000000000000007f << 48)
		result[t] = permutation{sig: p, mask: mask6_9_7, number: t}
		t++
		sig = (sig << 9) | (sig >> (64 - 9))
	}

	p = sig
	result[t] = permutation{sig: p, mask: mask6_10_8, number: t}
	t++
	p = (sig & 0xffc0003fffffffff) | (sig & 0x003fc00000000000 >> 8) | (sig & 0x00003fc000000000 << 8)
	result[t] = permutation{sig: p, mask: mask6_10_8, number: t}
	t++
	p = (sig & 0xffc03fc03fffffff) | (sig & 0x003fc00000000000 >> 16) | (sig & 0x0000003fc0000000 << 16)
	result[t] = permutation{sig: p, mask: mask6_10_8, number: t}
	t++
	p = (sig & 0xffc03fffc03fffff) | (sig & 0x003fc00000000000 >> 24) | (sig & 0x000000003fc00000 << 24)
	result[t] = permutation{sig: p, mask: mask6_10_8, number: t}
	t++
	p = (sig & 0xffc03fffffc03fff) | (sig & 0x003fc00000000000 >> 32) | (sig & 0x00000000003fc000 << 32)
	result[t] = permutation{sig: p, mask: mask6_10_8, number: t}
	t++
	p = (sig & 0xffc07fffffffc07f) | (sig & 0x003f800000000000 >> 40) | (sig & 0x0000000000003f80 << 40)
	result[t] = permutation{sig: p, mask: mask6_10_7, number: t}
	t++
	p = (sig & 0xffc07fffffffff80) | (sig & 0x003f800000000000 >> 47) | (sig & 0x000000000000007f << 47)
	result[t] = permutation{sig: p, mask: mask6_10_7, number: t}
	t++
	return result
}

// Add inserts a signature and document id into the store
func (s *Store6) Add(sig uint64, docid uint64) {

	s.docids = append(s.docids, entry{hash: sig, docid: docid})
	for _, v := range s.generate_permutations(sig) {
		s.rhashes[v.number].add(v.sig)
	}

}

func (*Store6) unshuffle(sig uint64, t int) uint64 {

	t7 := t % 7
	shift := 8 * uint64(t7)

	var m2 uint64

	if t < 42 {
		m2 = 0x007f800000000000

		if t7 == 6 {
			m2 = 0x007f000000000000
		}
	} else {
		m2 = 0x003fc00000000000

		if t7 >= 5 {
			m2 = 0x003f800000000000

			if t7 == 6 {
				shift--
			}
		}
	}

	m3 := uint64(m2 >> shift)
	m1 := ^uint64(0) &^ (m2 | m3)

	sig = (sig & m1) | (sig & m2 >> shift) | (sig & m3 << shift)
	sig = (sig >> (9 * (uint64(t) / 7))) | (sig << (64 - (9 * (uint64(t) / 7))))
	return sig
}

func (s *Store6) unshuffleList(sigs []uint64, t int) []uint64 {
	for i := range sigs {
		sigs[i] = s.unshuffle(sigs[i], t)
	}

	return sigs
}

const mask6_9_8 = 0xffff800000000000
const mask6_9_7 = 0xffff000000000000
const mask6_10_8 = 0xffffc00000000000
const mask6_10_7 = 0xffff800000000000

// Find searches the store for all hashes hamming distance 6 or less from the
// query signature.  It returns the associated list of document ids.

func (s *Store6) Search(in <-chan permutation) <-chan uint64 {
	out := make(chan uint64, 49)
	go func() {
		for v := range in {
			docs := s.unshuffleList(s.rhashes[v.number].find(v.sig, v.mask, 6), v.number)
			for _, d := range docs {
				out <- d
			}
		}
		close(out)
	}()
	return out
}

func (s *Store6) Find(sig uint64) []uint64 {

	// empty store
	if len(s.docids) == 0 {
		return nil
	}
	in := make(chan permutation, 49)
	for _, v := range s.generate_permutations(sig) {
		in <- v
	}
	close(in)

	out := make([]<-chan uint64, 4)
	for i := 0; i < 4; i++ {
		out[i] = s.Search(in)
	}

	var ids []uint64
	for _, subout := range out {
		for docid := range subout {
			ids = append(ids, docid)
		}
	}

	ids = unique(ids)

	var docids []uint64
	for _, v := range ids {
		docids = append(docids, s.docids.find(v)...)
	}

	return docids
}