package main

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var isHeaderRead = false

const (
	separator      = ";"
	position       = 1
	lenghtOfNumber = 12
)

func main() {
	filename := os.Args[1]
	start := time.Now()
	log.Println("starting read/write...")
	inFile, _ := os.Open(filename)
	defer inFile.Close()

	outFile, _ := os.OpenFile("f"+filename, os.O_RDWR, 0777)
	defer outFile.Close()

	log.Println("done. Time elapsed: ", time.Since(start))
}

func changeData(data string) string {
	log.Println("record", data)
	if !isHeaderRead {
		isHeaderRead = true
		return data
	} else {
		i := strings.Index(data, separator)
		number := data[i+1 : i+lenghtOfNumber]

		table := crc32.MakeTable(crc32.IEEE)
		checksum := crc32.Checksum([]byte(number), table)

		resultString := strings.Replace(data, number, string(checksum), 1)
		log.Println("result ->>:", resultString)
		return resultString
	}
}

func _t() {
	filename := os.Args[1]
	start := time.Now()
	log.Println("starting read/write...")

	wg := sync.WaitGroup{}
	wg.Add(50000)

	go func() {
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}

		for j := 0; j < 10; j++ {
			r := strings.NewReader(fmt.Sprintf("goroutine: %d", j))
			_, err = io.Copy(f, r)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = f.Close()
		if err != nil {
			log.Fatal(f)
		}
		wg.Done()
	}()

	wg.Wait()

	log.Println("done. Time elapsed: ", time.Since(start))
}

func _main() {
	filename := os.Args[1]
	start := time.Now()
	log.Println("starting read/write...")
	inFile, _ := os.Open(filename)
	defer inFile.Close()

	outFile, _ := os.OpenFile("f"+filename, os.O_RDWR, 0777)
	defer outFile.Close()

	reader := bufio.NewReaderSize(inFile, 10*2048)

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

func __main() {
	srt := "qwerty;79091111454;werwewwer;1231231"

	i := strings.Index(srt, separator)
	number := srt[i+1 : i+lenghtOfNumber]

	table := crc32.MakeTable(crc32.IEEE)
	checksum := crc32.Checksum([]byte(number), table)
	log.Println("Checksum:", checksum)

	resultString := strings.Replace(srt, number, string(checksum), 1)
	log.Println(resultString)

	// modify := srt[i+1 : i+12]
	// hash := "ooooooooo"
	// result := strings.Replace(srt, modify, hash, 2)

	// log.Println(modify)
	// log.Println(result)

	// s := "qwerty;79091111454;werwewwer;1231231"
	// first3 := s[0:3]
	// last3 := s[len(s)-3:]
	// log.Println(first3)
	// log.Println(last3)

	// first := strings.IndexAny(srt, ";")
	// tmp := srt[first+1:]             // 6 and last
	// log.Println(first)

	// d := tmp[first+1:]
	// log.Println(d)
	// second := strings.Index(tmp[first+1:], ";")
	// log.Println(second)
	// temp := tmp[first:second]

	// log.Println(first)
	// log.Println(second)
	// log.Println(temp)

	// rs := make[srt, len(srt)]
}
