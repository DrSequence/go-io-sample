package main

import (
	"bufio"
	"encoding/csv"
	"log"
	"os"
)

func main() {
	resultFile, err := os.OpenFile("large_example.csv", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal(err)
	}

	defer resultFile.Close()

	// todo: use wtitter...
	w := csv.NewWriter(bufio.NewWriter(resultFile))
	w.Comma = ';'

	// id;name;descr;secret
	header := [4]string{"id", "name", "descr", "secret"}

	w.Write(header)
	w.Flush()

	for i := 0; i < 100_000_000; i++ {
		var record []string
		record[0] = string(i)
		record[1] = "name"
		record[2] = "descr"
		record[3] = "secter"
		w.Write(record)
		w.Flush()
	}
}
