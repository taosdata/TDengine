package main

import (
	"fmt"
	"net/http"
)

func RootHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintln(w, "root handle")
}

func RestHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintln(w, "rest handle")
}

func LoginHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type","text/json")

	fmt.Fprintln(w, "{\"status\":\"succ\",\"code\":0,\"desc\":\"/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04\"}")
	fmt.Println("login handle")
}

func SqlHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "{\"status\":\"succ\",\"head\":[\"affected_rows\"],\"data\":[[0]],\"rows\":0}")
	//fmt.Println("rest handle")
}
 
func main() {
	http.HandleFunc("/", RootHandler)
	http.HandleFunc("/rest", RestHandler)
	http.HandleFunc("/rest/login", LoginHandler)
	http.HandleFunc("/rest/login/root", LoginHandler)
	http.HandleFunc("/rest/login/root/taosdata", LoginHandler)
	http.HandleFunc("/rest/sql", SqlHandler)
    http.ListenAndServe("127.0.0.1:6041", nil)
}