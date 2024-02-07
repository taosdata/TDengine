package main

import (
	"encoding/pem"
	"fmt"
	"os"
)

func main() {
	pemData, err := os.ReadFile("cert.pem")
	if err != nil {
		fmt.Println("Failed to read PEM file:", err)
		os.Exit(1)
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		fmt.Println("Failed to decode PEM data")
		os.Exit(1)
	}

	derData := block.Bytes

	err = os.WriteFile("cert.der", derData, 0644)
	if err != nil {
		fmt.Println("Failed to write DER file:", err)
		os.Exit(1)
	}

	fmt.Println("Conversion successful: PEM to DER")
}
