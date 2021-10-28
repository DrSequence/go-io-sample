package config

import (
	"io/ioutil"
	"log"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

var hashPosition = 0
var csvFile = "example.csv"
var newFile = "new.csv"

type Config struct {
	input    string `yaml:"input"`
	output   string `yaml:"output"`
	position string `yaml:"position"`
}

func InitConfig() (config Config) {
	filename, _ := filepath.Abs("config/config.yml")
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		panic(err)
	}
	if err != nil {
		log.Fatal(err)
	}

	log.Println("coinfig", config)
	log.Println("input file:", config.input)

	return config
}
