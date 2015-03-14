package vptree

type priorityQueue []*heapItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want a max-heap, so we use greater-than here
	return pq[i].Dist > pq[j].Dist
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(i interface{}) {
	item := i.(*heapItem)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq priorityQueue) Top() interface{} {
	return pq[0]
}
