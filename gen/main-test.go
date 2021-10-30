package main

import (
	"testing"
)

func TestRandateNumber(t *testing.T) {
	for i := 0; i < 1_000_000; i++ {
		result := RandateNumber()
		if len(result) != 11 {
			t.Error("RandateNumber")
		}
	}
}
