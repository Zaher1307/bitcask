package main

import (
	"log"

	resp "github.com/zaher1307/bitcask/pkg/respserver"
)

func main() {
	err := resp.StartServer()
	if err != nil {
		log.Fatal("error connection")
		return
	}
}
