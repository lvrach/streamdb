package main

import (
	"./store"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

var db store.DB

func main() {

	http.HandleFunc("/stream/", streamHandler)
	http.HandleFunc("/query/", queryHandler)
	http.Handle("/", http.FileServer(http.Dir("./static/example/")))

	http.ListenAndServe(":8080", nil)
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

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	cursor := db.Stream(streamId).Cursor()
	defer cursor.Close()

	close := w.(http.CloseNotifier).CloseNotify()

	sum := make(map[string]float64)

	for {

		select {
		case rows := <-cursor.Data():
			// Server Sent Events compatible

			for _, row := range rows {
				for _, field := range fields {
					num, err := row.Number(field)
					if err != nil {
						continue
					}
					sum[field] += num
				}
			}
			result, err := json.Marshal(sum)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", string(result))

			flusher.Flush()
		case <-close:
			log.Println("close query stream for", streamId)
			return
		}

	}
}

func streamHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path, "/")

	streamId := path[2]

	var data []store.Row
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&data); err != nil {
		log.Println(err)
		return
	}
	fmt.Fprintf(w, "OK!")
	db.Stream(streamId).Push(data)
}
