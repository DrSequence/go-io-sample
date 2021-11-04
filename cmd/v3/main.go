package main

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FilesConfig struct {
	position  int
	reader    *os.File
	overwrite *os.File
	hex       *os.File
}

type ProcessPool struct {
	linesPool  *sync.Pool
	stringPool *sync.Pool
}

var config FilesConfig

const (
	separator      = ";"
	bSeparator     = ';'
	position       = 1
	lenghtOfNumber = 12
)

func main() {
	startTime := time.Now()
	fileName := os.Args[1]

	position, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Panicln("cannot able to read the file", err)
	}
	defer file.Close()

	newFile, err := os.OpenFile("o.csv", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Panicln("cannot able to read overwrite file", err)
	}
	defer newFile.Close()

	hexFile, err := os.OpenFile("hex.txt", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Panicln("cannot able to read hex file", err)
	}
	defer hexFile.Close()
	config = FilesConfig{position, file, newFile, hexFile}
	Read(config)

	log.Println("\nTime taken - ", time.Since(startTime))
}

func Read(config FilesConfig) error {
	linesPool := sync.Pool{New: func() interface{} {
		return make([]byte, 250*1024)
	}}

	stringPool := sync.Pool{New: func() interface{} {
		return ""
	}}
	pp := ProcessPool{&linesPool, &stringPool}

	r := bufio.NewReader(config.reader)
	var wg sync.WaitGroup

	for {
		buf := linesPool.Get().([]byte)
		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}

		nextUntillNewline, err := r.ReadBytes('\n')
		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}

		wg.Add(1)
		go func() {
			ProcessChunk(buf, &pp)
			wg.Done()
		}()
	}

	wg.Wait()
	// endChan <- true
	return nil
}

func ProcessChunk(chunk []byte, pp *ProcessPool) {
	var wg2 sync.WaitGroup

	record := pp.stringPool.Get().(string)
	record = string(chunk)
	pp.linesPool.Put(chunk)

	recordSlice := strings.Split(record, "\n")
	pp.stringPool.Put(record)

	chunkSize := 300
	n := len(recordSlice)
	noOfThread := n / chunkSize
	if n%chunkSize != 0 {
		noOfThread++
	}

	for i := 0; i < noOfThread; i++ {
		wg2.Add(1)

		go func(s int, e int) {
			defer wg2.Done()

			for i := s; i < e; i++ {
				text := recordSlice[i]
				if len(text) == 0 {
					continue
				}
				ProcessRecord(text)

			}

		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(recordSlice)))))
	}

	wg2.Wait()
	recordSlice = nil
}

func ProcessRecord(text string) {
	// defer close(reChan)
	i := strings.Index(text, separator)
	var result []byte

	if i != -1 {
		number := text[i+1 : i+lenghtOfNumber]
		table := crc32.MakeTable(crc32.IEEE)
		checksum := crc32.Checksum([]byte(number), table)
		checkSumAsString := fmt.Sprintf("%02x", checksum)
		resultString := strings.Replace(text, number, checkSumAsString, 1)
		result = []byte(resultString)

		WriteHexPair([]byte(number), checkSumAsString)
	}

	Write([]byte(result))
}

func WriteHexPair(b []byte, checkSumAsString string) {
	b = append(b, bSeparator)
	b = append(b, checkSumAsString...)
	b = append(b, '\n')
	config.hex.Write(b)
}

func Write(b []byte) {
	b = append(b, '\n')
	config.overwrite.Write(b)
}
