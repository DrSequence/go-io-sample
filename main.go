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
var csvFile = "example.csv"
var newFile = "new.csv"
var wg sync.WaitGroup

type SyncWriter struct {
	m      sync.Mutex
	Writer io.Writer
}

func (w *SyncWriter) Write(b []byte) (n int, err error) {
	w.m.Lock()
	defer w.m.Unlock()

	return w.Writer.Write(b)
}

func parse(strArray []string) string {
	var resultString = ""
	for i, r := range strArray {
		if i != len(strArray)-1 {
			resultString += r + ";"
		} else {
			resultString += r
		}
	}
	return resultString
}

func init() {
	// Change the device for logging to stdout.
	log.SetOutput(os.Stdout)

	var err error
	hashPosition, err = strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalln(err)
	}

	csvFile = os.Args[1]
	newFile = os.Args[2]

	// add concurrency
	wg.Add(concurrency)
}

func main() {
	log.Println("starting...", time.Now())
	domainsChanel := make(chan string, 1)
	// todo....
	// shotdownChannel := make(chan string, 1)

	log.Println("filename:", csvFile)
	csvFile, err := os.Open(csvFile)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	log.Println("creating result file...")
	resultFile, err := os.OpenFile(newFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer resultFile.Close()

	log.Println("copy headers...")
	// copy headers from csv to new file
	r := csv.NewReader(bufio.NewReader(csvFile))
	w := csv.NewWriter(bufio.NewWriter(resultFile))
	r.Comma = comma
	r.Comment = comment
	w.Comma = comma

	// read header
	var headers []string
	headers, err = r.Read()
	if err != nil {
		log.Fatalln(err)
	}

	// read fields and push to channel
	go read(r, csvFile, domainsChanel)

	// mutex
	wr := &SyncWriter{sync.Mutex{}, resultFile}
	// write headers
	fmt.Fprintln(wr, parse(headers))
	for rec := range domainsChanel {
		wg.Add(1)
		go func(r string) {
			log.Println("|r| -> ", r)
			fmt.Fprintln(wr, r)
			defer wg.Done()
		}(rec)
	}

	wg.Wait()
	close(domainsChanel)
	log.Println("end...")
}

func read(r *csv.Reader, file *os.File, domains chan string) {
	defer file.Close()

	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(domains)
			log.Fatal(err)
		}
		log.Println("|reader| record:", record)

		arr := record
		h := sha1.New()
		h.Write([]byte(arr[hashPosition]))
		sha1_hash := hex.EncodeToString(h.Sum(nil))
		arr[hashPosition] = sha1_hash
		log.Println("|new record| ->", arr)

		domains <- parse(record)
	}
}
