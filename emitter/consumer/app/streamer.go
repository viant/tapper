package main

import (
	"fmt"
	"github.com/viant/tapper/emitter/consumer"
	"log"
	"os"
)

func main() {
	configURL := os.Getenv("CONFIG_URL")
	config, err := consumer.NewConfigFromURL(configURL)
	if err != nil {
		log.Fatal(err)
	}

	service, err := consumer.New(config)
	if err != nil {
		log.Fatal(err)
	}
	server := consumer.NewServer(config.Port, service)
	fmt.Printf("Starting streamer at :%v", config.Port)
	err = server.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}
