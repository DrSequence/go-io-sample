package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const comma = ';'

func RandateTime() string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min

	t := time.Unix(sec, 0)
	formatted := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())

	return formatted
}

func RandateNumber() string {
	min := 79000000000
	max := 79999999999
	val := rand.Intn(max-min) + min
	return strconv.Itoa(val)
}

func RandateSN() string {
	min := 1000000000
	max := 9999999999
	val := rand.Intn((max - min) + min)
	return strconv.Itoa(val)
}

func RandateInt(size int) string {
	start := 1
	end := 1

	for i := 0; i < size; i++ {
		start = start * 10
	}
	end = start * 10

	val := rand.Intn(end-start) + end
	return strconv.Itoa(val)
}

func RandateFloat() string {
	return fmt.Sprintf("%v", rand.Float64())
}

func main() {
	amout, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("first pararm is amount of fields. not found", err)
	}
	log.Println(fmt.Sprintf("starting generation file with %d fields", amout))
	resultFile, err := os.OpenFile(os.Args[2], os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal(err)
	}

	defer resultFile.Close()

	// todo: use wtitter...
	w := csv.NewWriter(bufio.NewWriter(resultFile))
	w.Comma = comma

	header := [6]string{
		"data", "need_hash_field", "one_more_field", "one_more_field",
		"one_more_field", "one_more_field",
	}

	w.Write(header[:])
	w.Flush()

	for i := 0; i < amout; i++ {
		record := [6]string{
			RandateTime(),
			RandateNumber(),
			RandateSN(),
			RandateInt(4),
			RandateInt(5),
			RandateFloat(),
		}

		w.Write(record[:])
		w.Flush()
	}
	log.Printf("end of writing...")
}
