package hasty_test

import (
	"fmt"
	"log"

	hasty "github.com/marselester/hastydb"
)

func Example() {
	db, close, err := hasty.Open("testdata/mydb")
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
	// Output:
	// Moist von Lipwig

	if err = close(); err != nil {
		log.Fatal(err)
	}
}
