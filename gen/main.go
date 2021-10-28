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

var comma = ';'

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
	max := 70000000000
	min := 80000000000
	val := rand.Intn((max - min) + min)
	return strconv.Itoa(val)
}

func RandateSN() string {
	max := 1000000000
	min := 9999999999
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

	val := rand.Intn((start - end) + end)
	return strconv.Itoa(val)
}

func RandateFloat() string {
	return fmt.Sprintf("%v", rand.Float64())
}

func main() {
	amout, _ := strconv.Atoi(os.Args[1])
	log.Println(fmt.Sprintf("starting generation file with %d fields", amout))
	resultFile, err := os.OpenFile("large_example.csv", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal(err)
	}

	defer resultFile.Close()

	// todo: use wtitter...
	w := csv.NewWriter(bufio.NewWriter(resultFile))
	w.Comma = comma

	header := [6]string{
		"data", "phone_number", "descr", "extra_field_here",
		"prepare_new_field_1", "one_more_extra_field",
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
