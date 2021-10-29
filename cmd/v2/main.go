package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var isHeaderRead = false
var position = 1

func main() {
	start := time.Now()
	log.Println("starting read/write...")
	inFile, _ := os.Open("csv/example.csv")
	defer inFile.Close()

	outFile, _ := os.OpenFile("csv/example.csv", os.O_RDWR, 0777)
	defer outFile.Close()

	reader := bufio.NewReaderSize(inFile, 10*1024)

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

func changeData(data string) string {
	// find fist and second ;
	// save positions: global - it's nessesary
	// find get substring
	// create hex
	// overwrite substring
	// return

	if !isHeaderRead {
		isHeaderRead = true
		return data
	} else {
		// o := make([]rune, 0, len(data))
		for _, record := range data {
			log.Println("record:", record)
		}

		return "\n"
	}

	// o := make([]rune, 0, len(data))
	// for _, r := range data {
	// 	if unicode.IsLetter(r) {
	// 		if unicode.IsUpper(r) {
	// 			o = append(o, unicode.ToLower(r))
	// 		} else {
	// 			o = append(o, unicode.ToUpper(r))
	// 		}
	// 	} else {
	// 		o = append(o, r)
	// 	}
	// }

}
