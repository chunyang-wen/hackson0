package main

import (
    "fmt"
    "log"
    flags "github.com/jessevdk/go-flags"
    router "github.com/chunyang-wen/hackson0/router"
    worker "github.com/chunyang-wen/hackson0/worker"
    common "github.com/chunyang-wen/hackson0/common"
)



func main() {
    var opts common.Options
    parser := flags.NewParser(&opts, flags.Default)
    _, err := parser.Parse()
    if err != nil {
        log.Fatal("Error when parsing args")
        fmt.Println(err)
    }
    if opts.Type == "W" {
        worker.Run(opts)
    } else {
        router.Run(opts)
    }
}
