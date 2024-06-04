package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func SingleHash(in, out chan interface{}) {
	hashFunc := func(val string) {
		left := make(chan string)
		right := make(chan string)

		crCalc := func(value string, output chan string) {
			output <- DataSignerCrc32(value)
		}
		mdCalc := func(value string, output chan string) {
			output <- DataSignerMd5(value)
		}

		go crCalc(val, left)
		go mdCalc(val, right)
		lValue := <-left
		rValue := <-right

		go crCalc(rValue, right)
		rValue = <-right

		out <- lValue + "~" + rValue
	}

	for data := range in {
		switch val := data.(type) {
		case int:
			hashFunc(strconv.Itoa(val))
		case string:
			hashFunc(val)
		default:
			fmt.Println("Unknown type")
		}
	}
}

const MultiHashTimes = 6

func MultiHash(in, out chan interface{}) {
	for data := range in {
		switch val := data.(type) {
		case string:
			hashes := make([]string, MultiHashTimes)
			mx := &sync.Mutex{}
			wg := &sync.WaitGroup{}
			for i := 0; i < MultiHashTimes; i++ {
				wg.Add(1)
				go func(value string, index int, output []string) {
					newHash := DataSignerCrc32(strconv.Itoa(index) + value)
					mx.Lock()
					output[index] = newHash
					mx.Unlock()
					wg.Done()
				}(val, i, hashes)
			}
			wg.Wait()

			multiHash := strings.Builder{}
			for i := 0; i < MultiHashTimes; i++ {
				mx.Lock()
				multiHash.WriteString(hashes[i])
				mx.Unlock()
			}
			out <- multiHash.String()
		}
	}
}

func CombineResults(in, out chan interface{}) {
	builder := strings.Builder{}
	for value := range in {
		switch str := value.(type) {
		case string:
			builder.WriteString(str)
			builder.WriteString("_")
		}
	}
	temp := builder.String()
	out <- temp[:len(temp)-1]
}

func ExecutePipeline(workers ...job) {
	in := make(chan interface{}, 2)
	wg := &sync.WaitGroup{}

	workFunc := func(f job, in chan interface{}) chan interface{} {
		output := make(chan interface{})
		go func() {
			defer close(output)
			defer wg.Done()
			f(in, output)
		}()
		return output
	}

	for _, worker := range workers {
		wg.Add(1)
		in = workFunc(worker, in)
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
			data, _ := dataRaw.(string)
			fmt.Println(data)
		}),
	}

	ExecutePipeline(hashSignJobs...)

	end := time.Now()
	fmt.Println(end.Sub(start).Seconds(), "sec")
}
