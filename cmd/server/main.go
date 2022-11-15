package main

import (
	"flag"
	"log"
	"os"

	resp "github.com/zaher1307/bitcask/pkg/respserver"
)

func main() {
	directoryFlag := flag.String("directory", os.Getenv("HOME")+"/resp_server_datastore", "the directory of db")
	listenPortFlag := flag.String("port", "6379", "the listen port")
	err := resp.StartServer(*directoryFlag, *listenPortFlag)
	if err != nil {
		log.Fatal("error connection")
		return
	}
}
