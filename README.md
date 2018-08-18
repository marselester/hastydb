# HastyDB 🚲

HastyDB is a key-value log-structured storage engine. Unlike [RascalDB](https://github.com/marselester/rascaldb)
it doesn't hold all keys in memory, instead it maintains a sparse index.
This is achieved by keeping a segment file in Sorted String Table (SSTable) format which means
key-value pairs are sorted by key and each key appears only once in the segment.

To retain high performance, records are added into a memtable (tree) which allows to insert keys in
any order and read them back in sorted order (that helps to persist records in SSTable format on disk).

Key points:

- [ ] writes go to a memtable (in-memory tree data structure, preferably self-balancing).
  Currently [Binary Search Tree](https://github.com/marselester/binary-search-tree) is used for simplicity.
- [ ] periodically write a memtable to a new SSTable file, new writes go to a new memtable
- [ ] segment files are periodically merged and compacted in background
- [ ] all writes into a memtable are appended to a log file, so when db crashes, a memtable is restored from it
- [ ] firstly reads are served from a memtable and then SSTable files are checked
- [ ] read requests are optimized with [Bloom filters](https://github.com/marselester/bloom)
