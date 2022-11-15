package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	resp "github.com/zaher1307/bitcask/pkg/respserver"
)

func main() {
	directoryFlag := flag.String("directory", os.Getenv("HOME")+"/resp_server_datastore", "the directory of db")
	listenPortFlagInt := flag.Int("port", 6379, "the listen port")
    flag.Parse()
    listenPortFlagString := fmt.Sprint(*listenPortFlagInt)
	err := resp.StartServer(*directoryFlag, listenPortFlagString)
	if err != nil {
		log.Fatal("error connection")
		return
	}
}
