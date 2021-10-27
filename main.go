package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

var hashPosition = 3
var comma = ';'
var comment = '#'

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
}

func main() {
	resultFile, err := os.OpenFile(os.Args[1], os.O_APPEND|os.O_RDWR, os.ModeAppend)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	wr := &SyncWriter{sync.Mutex{}, resultFile}
	wg := sync.WaitGroup{}

	r := csv.NewReader(bufio.NewReader(resultFile))
	r.Comment = comment
	r.Comma = comma

	// first line
	start := true

	for {
		if start == true {
			r.Read()
			start = false
			continue
		}

		wg.Add(1)

		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			wg.Done()
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		arr := record
		arr[hashPosition] = "new_record_yeap"
		fmt.Println("|new record| ->", arr)

		go func(r []string) {
			fmt.Fprintln(wr, r)
			wg.Done()
		}(record)
	}

	wg.Wait()
}

func _main() {
	log.Println("Start time", time.Now())

	domains := make(chan []string, 1)
	results := make(chan []string, 1)

	log.Println("opening file:" + os.Args[1])
	csvfile, err := os.Open(os.Args[1])

	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// close file but later
	defer csvfile.Close()

	resultFile, err := os.OpenFile(os.Args[1], os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalln("Couldn't open the csv file for writting...", err)
	}

	// close result file but later
	defer resultFile.Close()

	go read(csvfile, domains)
	go write(resultFile, domains)

	// create wg
	wg := new(sync.WaitGroup)
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		// parallel routine for cleansing
		go reculculate(domains, wg, results)
	}

	wg.Wait()
	fmt.Println("end time", time.Now())
}

func read(csvfile *os.File, domains chan []string) {
	// Parse the file
	r := csv.NewReader(bufio.NewReader(csvfile))
	r.Comment = '#'
	r.Comma = comma

	log.Println("reading file...")

	// skip first
	r.Read()
	// if err != nil {
	// 	log.Fatal(err)
	// }

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

		// arr := record
		// arr[hashPosition] = "new_record_yeap"
		// fmt.Println("|new record| ->", arr)
		// domains <- arr

		domains <- record
	}
}

func reculculate(domains <-chan []string, wg *sync.WaitGroup, results chan []string) {
	defer wg.Done()
	for domain := range domains {
		arr := domain
		arr[hashPosition] = "new_record_yeap"
		fmt.Println("|new record| ->", arr)
		results <- arr
	}
}

func write(csvfile *os.File, results chan []string) {
	log.Println("writing to file...")
	w := csv.NewWriter(bufio.NewWriter(csvfile))

	for result := range results {
		log.Println("|result| -> ", result)
		w.Write(result)
	}
	w.Flush()
}

// wr := &SyncWriter{sync.Mutex{}, csvfile}
// wg := sync.WaitGroup{}

// records, err := r.ReadAll()
// if err != nil {
// 	log.Fatal(err)
// }
// for _, val := range records {
// 	wg.Add(1)
// 	go func(greetings []string) {
// 		log.Println(greetings)
// 		fmt.Fprintln(wr, greetings)
// 		wg.Done()
// 	}(val)
// }

// wg.Wait()
