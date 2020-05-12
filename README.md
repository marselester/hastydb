# HastyDB 🚲

[![Documentation](https://godoc.org/github.com/marselester/hastydb?status.svg)](https://godoc.org/github.com/marselester/hastydb)
[![Go Report Card](https://goreportcard.com/badge/github.com/marselester/hastydb)](https://goreportcard.com/report/github.com/marselester/hastydb)

HastyDB is a key-value
[log-structured storage engine](https://go-talks.appspot.com/github.com/marselester/storage-engines/log-structured-engine.slide).

Note, this pet project is a proof of concept and it is not intended for production use.

Key points:

- [ ] writes go to a memtable (in-memory self-balancing binary search tree).
- [ ] periodically write a memtable to a new SSTable file, new writes go to a new memtable
- [ ] segment files are periodically merged and compacted in background
- [ ] all writes into a memtable are appended to a log file, so when db crashes, a memtable is restored from it
- [ ] firstly reads are served from a memtable and then SSTable files are checked
- [ ] read requests are optimized with [Bloom filters](https://github.com/marselester/bloom)

## Usage Example

```go
package main

import (
	"fmt"
	"log"

	hasty "github.com/marselester/hastydb"
)

func main() {
	db, close, err := hasty.Open("./mydb")
	if err != nil {
		log.Fatal(err)
	}

	name := []byte("Alice")
	if err = db.Set("name", name); err != nil {
		log.Fatal(err)
	}

	if name, err = db.Get("name"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", name)

	if err = close(); err != nil {
		log.Fatal(err)
	}
}
```
