package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"webhook/grafana_webhook"
)

func main() {
	addr := "0.0.0.0:9010"
	handler := http.DefaultServeMux
	handler.HandleFunc("/sms", grafana_webhook.HandleWebhook(func(w http.ResponseWriter, b *grafana_webhook.Body) {

		fmt.Printf("Grafana status: %s\n%s\n", b.Title, b.Message)
		// sendMessage(msg)

	}, 0))

	go http.ListenAndServe(addr, handler)
	log.Println(fmt.Sprintf("API is listening on: %s", addr))
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-quit
	log.Println("Shutdown WebServer ...")
}
