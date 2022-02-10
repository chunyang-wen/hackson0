package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	common "github.com/chunyang-wen/hackson0/common"
)

var store = make(map[string]string)

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
			hash, _ := store[message.ObjectId]
			result += hash
		} else {
			store[message.ObjectId] = message.Hash
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
	fmt.Printf("Run worker %s on port = %s\n", opts.Id, opts.Port)
	server := http.Server{
		Addr: "localhost:" + opts.Port,
	}
	handler := &Handler{Id: opts.Id}
	http.HandleFunc("/v1/messages", handler.handle)
	server.ListenAndServe()
}
