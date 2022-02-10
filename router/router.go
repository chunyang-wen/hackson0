package router

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	common "github.com/chunyang-wen/hackson0/common"
)

var meta_info = make(map[string]int)
var wg sync.WaitGroup

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

func FindBucket(pq *PriorityQueue, message common.StoreRequest) int {
	if bucket_id, exists := meta_info[message.ObjectId]; exists {
		return bucket_id
	}
	item := heap.Pop(pq)
	target := item.(*BucketStatus)
	target.bytes += int64(message.Size)
	heap.Push(pq, target)
	meta_info[message.ObjectId] = target.index
	return target.index
}

func ConsumeFile(name string, urls *[]string, pq *PriorityQueue) {
	reader, err := os.Open(name)
	if err != nil {
		log.Fatalf("Read Name = %s error, err = %v", name, err)
		panic("Read file error")
	}
	bufReader := bufio.NewReader(reader)
	content, _, _ := bufReader.ReadLine()
	count := 0
	messages := make(map[int][]common.StoreRequest)
	for len(content) != 0 {
		arrays := strings.Split(string(content), ",")
		content, _, _ = bufReader.ReadLine()
		message := common.StoreRequest{
			RequestId: arrays[0],
			Action:    arrays[1],
			ObjectId:  arrays[2],
		}
		if message.Action == "W" {
			message.Size, _ = strconv.Atoi(arrays[3])
			message.Hash = arrays[4]
		}
		bucket_id := FindBucket(pq, message)
		messages[bucket_id] = append(messages[bucket_id], message)
		count += 1
		if count%1000 == 0 {
			wg.Add(1)
			go func(m map[int][]common.StoreRequest) {
				defer wg.Done()
				PostRequest(urls, m)
			}(messages)
			messages = make(map[int][]common.StoreRequest)
		}
		if count%1000000 == 0 {
			fmt.Printf("Processed to %d\n", count)
		}
	}
	if len(messages) != 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			PostRequest(urls, messages)
		}()
	}
	fmt.Printf("Processed to %d\n", count)
}

func PostRequest(url *[]string, message map[int][]common.StoreRequest) {
	for bucket_id, messages := range message {
		postBody, _ := json.Marshal(messages)
		fmt.Println("Get url: ", url)
		fmt.Println("Post message: ", messages)
		resp, err := http.Post((*url)[bucket_id], "application/json", bytes.NewBuffer(postBody))
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
}

func Run(opts common.Options) {
	fmt.Println("Run router")
	pq := make(PriorityQueue, opts.Num)
	urls := make([]string, opts.Num)
	wg.Add(1)
	for i := 0; i < opts.Num; i++ {
		pq[i] = &BucketStatus{Id: strconv.Itoa(i), bytes: 0, index: i}
		port, _ := strconv.Atoi(opts.Port)
		port += i
		url := "http://localhost:" + strconv.Itoa(port) + "/v1/messages"
		urls[i] = url
	}
	heap.Init(&pq)
	ConsumeFile(opts.File, &urls, &pq)
	wg.Done()
	wg.Wait()
}
