package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"unicode"
)

func main() {
	inFile, _ := os.Open("example.csv")
	defer inFile.Close()

	outFile, _ := os.OpenFile("example.csv", os.O_RDWR, 0777)
	defer outFile.Close()

	reader := bufio.NewReaderSize(inFile, 10*1024)

	for {
		line, err := reader.ReadString('\n')
		log.Println("line:", line)
		outFile.WriteString(changeData(line))
		if err != nil {
			if err != io.EOF {
				fmt.Println("error:", err)
			}
			break
		}
	}
}

func changeData(data string) string {
	log.Println("data:", data)

	o := make([]rune, 0, len(data))
	for _, r := range data {
		if unicode.IsLetter(r) {
			if unicode.IsUpper(r) {
				o = append(o, unicode.ToLower(r))
			} else {
				o = append(o, unicode.ToUpper(r))
			}
		} else {
			o = append(o, r)
		}
	}
	return string(o)
}
