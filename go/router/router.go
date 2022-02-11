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
	"time"

	common "github.com/chunyang-wen/hackson0/common"
)

var l = log.New(os.Stderr, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

var meta_info = make(map[string]int)
var wg sync.WaitGroup
var wg_r_w sync.WaitGroup

type BucketStatus struct {
	bytes int64
	Id    int
}

type PriorityQueue []*BucketStatus

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
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
	meta_info[message.ObjectId] = target.Id
	return target.Id
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
	read_messages := make(map[int][]common.StoreRequest)
	write_messages := make(map[int][]common.StoreRequest)
	for len(content) != 0 {
		arrays := strings.Split(string(content), ",")
		content, _, _ = bufReader.ReadLine()
		message := common.StoreRequest{
			RequestId: arrays[0],
			Action:    arrays[1],
			ObjectId:  arrays[2],
		}
		if message.Action == "W" {
			size, _ := strconv.Atoi(arrays[3])
			message.Size = size
			message.Hash = arrays[4]
			bucket_id := FindBucket(pq, message)
			write_messages[bucket_id] = append(write_messages[bucket_id], message)
		} else {
			bucket_id := FindBucket(pq, message)
			read_messages[bucket_id] = append(read_messages[bucket_id], message)
		}
		count += 1
		if count%10000 == 0 {
			if len(write_messages) != 0 {
				wg.Add(1)
				wg_r_w.Add(1)
				go func(m map[int][]common.StoreRequest) {
					defer wg.Done()
					defer wg_r_w.Done()
					PostRequest(urls, m, "W")
				}(write_messages)
			}
			wg_r_w.Wait()
			//l.Printf("Write message successfully\n")
			if len(read_messages) != 0 {
				wg.Add(1)
				go func(m map[int][]common.StoreRequest) {
					defer wg.Done()
					PostRequest(urls, m, "R")
				}(read_messages)
			}
			read_messages = make(map[int][]common.StoreRequest)
			write_messages = make(map[int][]common.StoreRequest)
		}
		if count%1000000 == 0 {
			l.Printf("Processed to %d\n", count)
		}
	}
	if len(write_messages) != 0 {
		wg.Add(1)
		wg_r_w.Add(1)
		go func(m map[int][]common.StoreRequest) {
			defer wg.Done()
			defer wg_r_w.Done()
			PostRequest(urls, m, "W")
		}(write_messages)
	}

	wg_r_w.Wait()
	if len(read_messages) != 0 {
		wg.Add(1)
		go func(m map[int][]common.StoreRequest) {
			defer wg.Done()
			PostRequest(urls, m, "R")
		}(read_messages)
	}
	if len(read_messages)+len(write_messages) != 0 {
		l.Printf("Processed to %d\n", count)
	}
	read_messages = make(map[int][]common.StoreRequest)
	write_messages = make(map[int][]common.StoreRequest)

}

func PostRequest(url *[]string, message map[int][]common.StoreRequest, action string) {
	for bucket_id, messages := range message {
		wg.Add(1)
		if action == "W" {
			wg_r_w.Add(1)
		}
		go func(bid int, m []common.StoreRequest) {
			defer wg.Done()
			if action == "W" {
				defer wg_r_w.Done()
			}
			postBody, _ := json.Marshal(m)
			//l.Println("Get url: ", url)
			//l.Println("Post message: ", messages)
			resp, err := http.Post((*url)[bid], "application/json", bytes.NewBuffer(postBody))
			if err != nil {
				l.Fatalf("An error %v", err)
			}
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			var response common.StoreResponse
			_ = json.Unmarshal(body, &response)
			for _, result := range response.Result {
				fmt.Println(result)
			}
		}(bucket_id, messages)

	}
}

func Run(opts common.Options) {
	l.Println("Run router")
	start_time := time.Now()
	pq := make(PriorityQueue, opts.Num)
	urls := make([]string, opts.Num)
	wg.Add(1)
	for i := 0; i < opts.Num; i++ {
		pq[i] = &BucketStatus{Id: i, bytes: 0}
		port, _ := strconv.Atoi(opts.Port)
		port += i
		url := "http://localhost:" + strconv.Itoa(port) + "/v1/messages"
		urls[i] = url
	}
	heap.Init(&pq)
	ConsumeFile(opts.File, &urls, &pq)
	wg.Done()
	wg.Wait()
	for pq.Len() != 0 {
		item := heap.Pop(&pq).(*BucketStatus)
		l.Printf("Bucket id: %d, bytes: %d", item.Id, item.bytes)
	}
	end_time := time.Now()
	l.Println(end_time.Sub(start_time))
}
