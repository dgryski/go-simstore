go-simstore: store and search through simhashes

This package is an implementation of section 3 of "Detecting Near-Duplicates
for Web Crawling" by Manku, Jain, and Sarma,

http://www2007.org/papers/paper215.pdf

* simhash is a simple simhashing library.
* simstore is the storage and saerching logic
* simd is a small daemon that wraps simstore and exposes a http /search endpoint
