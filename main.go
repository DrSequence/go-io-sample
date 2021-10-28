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
	"strings"
	"sync"
	"time"
)

var hashPosition = 0

var comma = ';'
var comment = '#'
var csvFile = "example.csv"
var newFile = "new.csv"

var concurrency = 500

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
	var sb strings.Builder
	for i, r := range strArray {
		if i != len(strArray)-1 {
			sb.WriteString(r + ";")
		} else {
			sb.WriteString(r)
		}
	}
	return sb.String()
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
	wg.Add(concurrency)
}

func main() {
	log.Println("starting...", time.Now())
	dChannel := make(chan string, 1000)

	termChan := make(chan os.Signal)
	// _, cancelFunc := context.WithCancel(context.Background())
	// signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("filename:", csvFile)
	csvFile, err := os.Open(csvFile)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	defer csvFile.Close()

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

	go read(r, dChannel)
	go write(resultFile, dChannel, headers)

	<-termChan
	wg.Wait()
	log.Println("end...")
}

func read(r *csv.Reader, dChannel chan string) {
	defer close(dChannel)

	log.Println("start reading...")
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(dChannel)
			log.Fatal(err)
		}

		arr := record
		h := sha1.New()
		h.Write([]byte(arr[hashPosition]))
		sha1_hash := hex.EncodeToString(h.Sum(nil))
		arr[hashPosition] = sha1_hash
		log.Println("reader|new record| ->", arr)
		dChannel <- parse(record)
	}

	log.Println("end if reading...")
}

func write(resultFile *os.File, dChannel chan string, headers []string) {
	// mutex
	wr := &SyncWriter{sync.Mutex{}, resultFile}
	// write headers
	fmt.Fprintln(wr, parse(headers))

	for rec := range dChannel {
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			log.Println("|r| -> ", r)
			fmt.Fprintln(wr, r)
		}(rec)
	}
}
