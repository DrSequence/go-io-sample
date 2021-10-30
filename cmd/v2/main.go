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

func changeData(data string) string {
	log.Println("record", data)
	if !isHeaderRead {
		isHeaderRead = true
		return data
	} else {
		i := strings.Index(data, separator)
		if i != -1 {
			number := data[i+1 : i+lenghtOfNumber]

			table := crc32.MakeTable(crc32.IEEE)
			checksum := crc32.Checksum([]byte(number), table)

			resultString := strings.Replace(data, number, string(checksum), 1)
			log.Println("result ->>:", resultString)
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
	inFile, _ := os.Open(filename)
	defer inFile.Close()

	outFile, _ := os.OpenFile("f"+filename, os.O_RDWR, 0777)
	defer outFile.Close()

	reader := bufio.NewReaderSize(inFile, 1024)

	for {
		line, err := reader.ReadString('\n')
		log.Println("|readliner|line|:", line)
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
