package main

import (
	"fmt"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func checkErr(err error, prompt string) {
	if err != nil {
		fmt.Printf("%s\n", prompt)
		panic(err)
	}
}

func main() {
	_, err := wrapper.TaosConnect("localhost", "root", "taosdata", "", 6030)
	checkErr(err, "fail to connect")

}
