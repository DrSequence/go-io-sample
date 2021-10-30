package config

import (
	"log"
	"testing"
)

func TestConfig(t *testing.T) {
	config := InitConfig()
	log.Println(config.input)
}
