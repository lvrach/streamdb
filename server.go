package main

import (
	"./store"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

var db *store.DB

func main() {

	db = store.Open("default")
	defer db.Close()

	http.HandleFunc("/stream/", streamHandler)
	http.HandleFunc("/query/", queryHandler)
	http.Handle("/", http.FileServer(http.Dir("./static/example/")))

	fmt.Println("listening at 8080")

	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		panic(err)
	}
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)

	path := strings.Split(r.URL.Path, "/")
	query := r.URL.Query()

	streamId := path[2]
	fields := query["field"]

	log.Println("new query stream for", streamId)

	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	header := w.Header()

	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("Access-Control-Allow-Origin", "*")

	sum := NewSum(fields, func(sum interface{}) {
		result, err := json.Marshal(sum)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", string(result))

		flusher.Flush()

	})

	cursor := db.Stream(streamId).Iteratorate(sum)

	defer cursor.Close()

	<-w.(http.CloseNotifier).CloseNotify()
	log.Println("close query stream for", streamId)
	return
}

func streamHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path, "/")

	streamId := path[2]

	var row []store.Row
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&row); err != nil {
		log.Println(err)
		return
	}
	fmt.Fprintf(w, "OK!")
	err := db.Stream(streamId).Push(row)
	if err != nil {
		fmt.Println(err)
	}
}

type Sum struct {
	fields []string
	onData func(interface{})
	result map[string]float64
}

func NewSum(fields []string, onData func(interface{})) (s *Sum) {

	result := make(map[string]float64)

	for _, field := range fields {
		result[field] = 0
	}

	return &Sum{
		fields,
		onData,
		result,
	}

}

func (s *Sum) Data(row store.Row) {
	for _, field := range s.fields {
		num, err := row.Number(field)
		if err != nil {
			continue
		}
		s.result[field] += num
	}
}

func (s *Sum) Flush() {
	s.onData(s.result)
}
