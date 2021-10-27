package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var hashPosition = 0
var comma = ';'
var comment = '#'
var concurrency = 500

type SyncWriter struct {
	m      sync.Mutex
	Writer io.Writer
}

func (w *SyncWriter) Write(b []byte) (n int, err error) {
	w.m.Lock()
	defer w.m.Unlock()
	return w.Writer.Write(b)
}

func init() {
	// Change the device for logging to stdout.
	log.SetOutput(os.Stdout)

	var err error
	hashPosition, err = strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalln(err)
	}
}

func recalculate(records <-chan []string, wg *sync.WaitGroup, results chan []string) {
	defer wg.Done()
	for record := range records {
		arr := record
		h := sha1.New()
		h.Write([]byte(arr[hashPosition]))
		sha1_hash := hex.EncodeToString(h.Sum(nil))
		arr[hashPosition] = sha1_hash
		log.Println("|new record| ->", arr)
		results <- arr
	}
}

func main() {
	log.Println("starting...", time.Now())
	domains := make(chan []string, 1000)
	results := make(chan []string, 1000)

	log.Println("filename:", os.Args[1])
	csvFile, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	log.Println("creating result file...")
	resultFile, err := os.OpenFile(os.Args[2], os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal(err)
	}

	defer csvFile.Close()

	log.Println("copy headers...")
	// copy headers from csv to new file
	r := csv.NewReader(bufio.NewReader(csvFile))

	// comma and comment settings
	r.Comma = comma
	r.Comment = comment

	// read header
	var headers []string
	headers, err = r.Read()
	if err != nil {
		log.Fatalln(err)
	}

	go read(r, domains)

	wg := new(sync.WaitGroup)
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go recalculate(domains, wg, results)
	}

	go write(resultFile, results, wg, headers)

	// wg.Wait()
	close(results)
}

func read(r *csv.Reader, domains chan []string) {
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			close(domains)
			break
		}
		if err != nil {
			close(domains)
			log.Fatal(err)
		}
		log.Println("|reader| record:", record)
		domains <- record
	}
}

func write(file *os.File, results chan []string, wg *sync.WaitGroup, headers []string) {
	defer file.Close()

	wr := &SyncWriter{sync.Mutex{}, file}
	// wg := sync.WaitGroup{}

	for result := range results {
		log.Println("|result| -> ", result)

		wg.Add(1)
		go func(r []string) {
			log.Println("|r| -> ", r)

			fmt.Fprintln(wr, r)
			wg.Done()
		}(result)
	}

	wg.Wait()
}
