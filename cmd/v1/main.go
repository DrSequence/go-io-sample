package main

import (
	"bufio"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"hash/crc32"
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
var startTime time.Time

type SyncWriter struct {
	m       sync.Mutex
	oWriter io.Writer
}

func (w *SyncWriter) Write(b []byte) (n int, err error) {
	w.m.Lock()
	defer w.m.Unlock()

	return w.oWriter.Write(b)
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
	startTime = time.Now()

	// Change the device for logging to stdout.
	log.SetOutput(os.Stdout)

	csvFile = os.Args[1]
	newFile = os.Args[2]
	var err error
	hashPosition, err = strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	log.Println("starting...")
	dChannel := make(chan string, 1024*64)
	var wgm sync.WaitGroup
	termChan := make(chan bool)

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

	log.Println("copy headers...")
	// copy headers from csv to new file
	r := csv.NewReader(bufio.NewReader(csvFile))
	r.Comma = comma
	r.Comment = comment

	// read header
	var headers []string
	headers, err = r.Read()
	if err != nil {
		log.Fatalln(err)
	}

	go read(r, dChannel)
	go write(resultFile, dChannel, headers, &wgm, termChan)

	<-termChan
	log.Println("ended.")
	log.Println("total time:", time.Since(startTime))
}

func read(r *csv.Reader, dChannel chan string) {
	defer close(dChannel)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(dChannel)
			log.Fatal(err)
		}

		var hasher string
		hash := crc32.NewIEEE()
		hashInBytes := hash.Sum(nil)[:]
		hasher = hex.EncodeToString(hashInBytes)

		record[hashPosition] = hasher
		log.Println("reader|new record| ->", record)
		dChannel <- parse(record)
	}
}

func write(resultFile *os.File, dChannel chan string, headers []string, wg *sync.WaitGroup, termChan chan bool) {
	defer resultFile.Close()

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
	wg.Wait()
	termChan <- true
}
