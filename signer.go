package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func getChannelCrcData(data string) chan string {
	out := make(chan string, 1)
	go func(output chan string) {
		output <- DataSignerCrc32(data)
	}(out)
	return out
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	hashFunc := func(val string, md5 string) {
		defer wg.Done()
		left := getChannelCrcData(val)
		rValue := DataSignerCrc32(md5)
		lValue := <-left
		out <- lValue + "~" + rValue
	}

	for data := range in {
		wg.Add(1)
		switch val := data.(type) {
		case int:
			str := strconv.Itoa(val)
			md5 := DataSignerMd5(str)
			go hashFunc(str, md5)
		case string:
			wg.Add(1)
			md5 := DataSignerMd5(val)
			go hashFunc(val, md5)
		default:
			fmt.Println("Unknown type")
			wg.Done()
		}
	}
	wg.Wait()
}

const MultiHashTimes = 6

func MultiHash(in, out chan interface{}) {
	wgFunc := &sync.WaitGroup{}
	for data := range in {
		switch val := data.(type) {
		case string:
			wgFunc.Add(1)
			go func(value string) {
				defer wgFunc.Done()
				hashes := make([]string, MultiHashTimes)
				mx := &sync.Mutex{}
				wgHash := &sync.WaitGroup{}
				for i := 0; i < MultiHashTimes; i++ {
					wgHash.Add(1)
					go func(index int) {
						newHash := DataSignerCrc32(strconv.Itoa(index) + value)
						mx.Lock()
						hashes[index] = newHash
						mx.Unlock()
						wgHash.Done()
					}(i)
				}
				wgHash.Wait()

				multiHash := strings.Builder{}
				for i := 0; i < MultiHashTimes; i++ {
					mx.Lock()
					multiHash.WriteString(hashes[i])
					mx.Unlock()
				}
				out <- multiHash.String()
			}(val)
		}
	}
	wgFunc.Wait()
}

func CombineResults(in, out chan interface{}) {
	var combinedStr []string
	for value := range in {
		switch str := value.(type) {
		case string:
			combinedStr = append(combinedStr, str)
		}
	}
	sort.Strings(combinedStr)
	out <- strings.Join(combinedStr, "_")
}

func ExecutePipeline(workers ...job) {
	in := make(chan interface{}, 2)
	wg := &sync.WaitGroup{}

	for _, worker := range workers {
		wg.Add(1)
		out := make(chan interface{})
		go func(f job, workIn, workOut chan interface{}) {
			defer wg.Done()
			defer close(workOut)
			f(workIn, workOut)
		}(worker, in, out)
		in = out
	}
	wg.Wait()
}

func main() {
	start := time.Now()

	inputData := []int{0, 1, 1, 2, 3, 5, 8}
	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			fmt.Println(dataRaw)
			data, _ := dataRaw.(string)
			fmt.Println(data)
		}),
	}

	ExecutePipeline(hashSignJobs...)

	end := time.Now()
	fmt.Println(end.Sub(start).Seconds(), "sec")
}
