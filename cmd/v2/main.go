package main

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var isHeaderRead = false

const (
	separator      = ";"
	position       = 1
	lenghtOfNumber = 12
)

func DefineType(filename string) (file *os.File, err error) {
	wType := os.Args[2]
	log.Println("type of overwrite:", wType)
	switch wType {
	case "o":
		return os.OpenFile(filename, os.O_RDWR, 0777)
	default:
		return os.OpenFile("new_file.csv", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	}
}

func changeData(data string) string {
	// log.Println("record", data)
	if !isHeaderRead {
		isHeaderRead = true
		return data
	} else {
		i := strings.Index(data, separator)
		if i != -1 {
			number := data[i+1 : i+lenghtOfNumber]

			table := crc32.MakeTable(crc32.IEEE)
			checksum := crc32.Checksum([]byte(number), table)

			resultString := strings.Replace(data, number, fmt.Sprintf("%02x", checksum), 1)
			// log.Println("result ->>:", resultString)
			return resultString
		} else {
			return data
		}
	}
}

func main() {
	filename := os.Args[1]
	start := time.Now()
	log.Println("starting read/write...")
	inFile, err := os.Open(filename)
	if err != nil {
		log.Fatal("Cannot open file!")
	}

	defer inFile.Close()

	outFile, err := DefineType(filename)
	if err != nil {
		log.Fatal("Cannot open file for writing!", err)
	}
	defer outFile.Close()

	reader := bufio.NewReaderSize(inFile, 1024)

	for {
		line, err := reader.ReadString('\n')
		// log.Println("|readliner|line|:", line)
		outFile.WriteString(changeData(line))
		if err != nil {
			if err != io.EOF {
				fmt.Println("error:", err)
			}
			break
		}
	}

	log.Println("done. Time elapsed: ", time.Since(start))
}
