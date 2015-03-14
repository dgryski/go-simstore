package vptree

import (
	"container/heap"
	"testing"
)

// This helper function compares two sets of coordinates/distances to make sure
// they are the same.
func compareCoordDistSets(t *testing.T, actualCoords []Item, expectedCoords []Item, actualDists, expectedDists []float64) {
	if len(actualCoords) != len(expectedCoords) {
		t.Fatalf("Expected %v coordinates, got %v", len(expectedCoords), len(actualCoords))
	}

	if len(actualDists) != len(expectedDists) {
		t.Fatalf("Expected %v distances, got %v", len(expectedDists), len(actualDists))
	}

	for i := 0; i < len(actualCoords); i++ {
		if actualCoords[i] != expectedCoords[i] {
			t.Fatalf("Expected actualCoords[%v] to be %x, got %x", i, expectedCoords[i], actualCoords[i])
		}
		if actualDists[i] != expectedDists[i] {
			t.Fatalf("Expected actualDists[%v] to be %v, got %v", i, expectedDists[i], actualDists[i])
		}
	}
}

// This helper function finds the k nearest neighbours of target in items. It's
// slower than the VPTree, but its correctness is easy to verify, so we can
// test the VPTree against it.
func nearestNeighbours(target uint64, items []Item, k int) (coords []Item, distances []float64) {
	pq := &priorityQueue{}

	// Push all items onto a heap
	for _, v := range items {
		d := hamming(v.Sig, target)
		heap.Push(pq, &heapItem{v, d})
	}

	// Pop all but the k smallest items
	for pq.Len() > k {
		heap.Pop(pq)
	}

	// Extract the k smallest items and distances
	for pq.Len() > 0 {
		hi := heap.Pop(pq)
		coords = append(coords, hi.(*heapItem).Item)
		distances = append(distances, hi.(*heapItem).Dist)
	}

	// Reverse coords and distances, because we popped them from the heap
	// in large-to-small order
	for i, j := 0, len(coords)-1; i < j; i, j = i+1, j-1 {
		coords[i], coords[j] = coords[j], coords[i]
		distances[i], distances[j] = distances[j], distances[i]
	}

	return
}

// This test makes sure vptree's behavior is sane with no input items
func TestEmpty(t *testing.T) {
	vp := New(nil)
	qp := uint64(0)

	coords, distances := vp.Search(qp, 3)

	if len(coords) != 0 {
		t.Error("coords should have been of length 0")
	}

	if len(distances) != 0 {
		t.Error("distances should have been of length 0")
	}
}

// This test creates a small VPTree and makes sure its search function returns
// the right results
func TestSmall(t *testing.T) {
	items := []Item{
		Item{0xdeadbeef, 57},
		Item{0xcabba9e5, 28},
		Item{0xcafebabe, 48},
		Item{0xc0cac0ca, 42},
	}

	target := uint64(0xcafef00d)

	itemsCopy := make([]Item, len(items))
	copy(itemsCopy, items)

	vp := New(itemsCopy)

	coords1, distances1 := vp.Search(target, 3)
	coords2, distances2 := nearestNeighbours(target, items, 3)

	compareCoordDistSets(t, coords1, coords2, distances1, distances2)
}
