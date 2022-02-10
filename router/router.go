package router

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	common "github.com/chunyang-wen/hackson0/common"
)

var meta_info = make(map[string]string)

type BucketStatus struct {
	bytes int64
	Id    string
	index int
}

type PriorityQueue []*BucketStatus

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].bytes < pq[j].bytes
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*BucketStatus)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func Run(opts common.Options) {
	fmt.Println("Run router")
	pq := make(PriorityQueue, opts.Num)
	urls := make([]string, opts.Num)
	for i := 0; i < opts.Num; i++ {
		pq[i] = &BucketStatus{Id: strconv.Itoa(i), bytes: 0, index: i}
		port, _ := strconv.Atoi(opts.Port)
		port += i
		url := "http://localhost:" + strconv.Itoa(port) + "/v1/messages"
		urls[i] = url
	}
	message := []common.StoreRequest{
		common.StoreRequest{ObjectId: "zz", RequestId: "0", Hash: "xyz", Size: 100, Action: "W"},
	}
	heap.Init(&pq)
	postBody, _ := json.Marshal(message)
	fmt.Println("Get url: ", urls[0])
	resp, err := http.Post(urls[0], "application/json", bytes.NewBuffer(postBody))
	if err != nil {
		log.Fatalf("An error %v", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var messages common.StoreResponse
	_ = json.Unmarshal(body, &messages)
	for _, message := range messages.Result {
		fmt.Println(message)
	}

}
