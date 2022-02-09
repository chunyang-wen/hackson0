package router

import (
    "fmt"
    common "github.com/chunyang-wen/hackson0/common"
)

func Run(opts common.Options) {
	fmt.Println("Run router")
    fmt.Printf("%s\n", opts.Type)
}
