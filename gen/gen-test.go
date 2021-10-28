package main

import (
	"testing"
)

func TestRandateNumber(t *testing.T) {
	result := RandateNumber()
	if len(result) != 11 {
		t.Error("RandateNumber")
	}
}

func TestRandateInt(it *testing.T) {
	// result := RandateInt(2)

	// t.Error("blaaa")
	// strconv.Itoa(rand.Intn(max-min) + min)
}
