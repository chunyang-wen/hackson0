package main

import (
	"fmt"
	"log"
	"os"

	common "github.com/chunyang-wen/hackson0/common"
	router "github.com/chunyang-wen/hackson0/router"
	worker "github.com/chunyang-wen/hackson0/worker"
	flags "github.com/jessevdk/go-flags"
)

func main() {
	var opts common.Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		log.Fatal("Error when parsing args")
		fmt.Println(err)
		os.Exit(1)
	}
	if opts.Type == "W" {
		worker.Run(opts)
	} else {
		router.Run(opts)
	}
}
