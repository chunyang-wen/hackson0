package worker

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	common "github.com/chunyang-wen/hackson0/common"
)

var store = make(map[string]string)
var m sync.RWMutex
var l = log.New(os.Stderr, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

type Handler struct {
	Id string
}

func (handler *Handler) handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	var messages []common.StoreRequest
	_ = json.Unmarshal(body, &messages)
	var results []string
	for _, message := range messages {
		result := message.RequestId
		result += ","
		if message.Action == "R" {
			m.RLock()
			hash, ok := store[message.ObjectId]
			m.RUnlock()
			if !ok {
				l.Fatalf("%s not existed\n", message.ObjectId)
			}
			result += hash
		} else {
			m.Lock()
			l.Printf("Write %s to store\n", message.ObjectId)
			store[message.ObjectId] = message.Hash
			m.Unlock()
			result += handler.Id
		}
		results = append(results, result)
		//fmt.Printf("Action = %s Result = %s\n", message.Action, result)
	}
	w.Header().Set("Content-Type", "application/json")
	resp := &common.StoreResponse{
		Result: results,
	}
	json, _ := json.Marshal(resp)
	w.WriteHeader(200) // Set status
	w.Write(json)

}

func Run(opts common.Options) {
	l.Printf("Run worker %s on port = %s\n", opts.Id, opts.Port)
	server := http.Server{
		Addr: "localhost:" + opts.Port,
	}
	handler := &Handler{Id: opts.Id}
	http.HandleFunc("/v1/messages", handler.handle)
	err := server.ListenAndServe()
	if err != nil {
		l.Fatal("Error: %v", err)
	}
}
