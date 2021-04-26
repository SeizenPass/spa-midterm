package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const multihashConstant = 6

// сюда писать код
func ExecutePipeline(jobs ...job)  {
	mainWaitGroup := &sync.WaitGroup{}
	input := make(chan interface{})
	for _, job := range jobs {
		mainWaitGroup.Add(1)
		output := make(chan interface{})
		go asyncWorker(mainWaitGroup, input, output, job)
		input = output
	}
	mainWaitGroup.Wait()
}

func asyncWorker(mainWaitGroup *sync.WaitGroup, input, output chan interface{}, job job)  {
	job(input, output)
	close(output)
	mainWaitGroup.Done()
}

func SingleHash(in, out chan interface{}) {
	mutex := &sync.Mutex{}
	singleHashWaitGroup := &sync.WaitGroup{}
	for data := range in {
		singleHashWaitGroup.Add(1)
		go asyncSingleHash(data, out, mutex, singleHashWaitGroup)
	}
	singleHashWaitGroup.Wait()
}

func asyncSingleHash(data interface{}, output chan interface{}, mutex *sync.Mutex, group *sync.WaitGroup)  {
	mutex.Lock()
	md5data := DataSignerMd5(strconv.Itoa(data.(int)))
	mutex.Unlock()
	asyncHashChannel := make(chan string)
	go func(output chan string, data interface{}) {
		output <- DataSignerCrc32(strconv.Itoa(data.(int)))
	}(asyncHashChannel, data)
	md5crc32data := DataSignerCrc32(md5data)
	crc32data := <-asyncHashChannel
	output <- crc32data + "~" + md5crc32data
	group.Done()
}

func MultiHash(in, out chan interface{}) {
	mwg := &sync.WaitGroup{}
	for data := range in {
		mwg.Add(1)
		go asyncMultiHash(data, out, mwg)
	}
	mwg.Wait()
}

func asyncMultiHash(data interface{}, output chan interface{}, group *sync.WaitGroup) {
	var outArray = make([]string, multihashConstant, multihashConstant)
	multiHashWaitGroup := &sync.WaitGroup{}
	for i := 0; i < multihashConstant; i++ {
		multiHashWaitGroup.Add(1)
		nextData := strconv.Itoa(i) + data.(string)
		go func(index int, group *sync.WaitGroup, data string) {
			defer group.Done()
			outArray[index] = DataSignerCrc32(data)
		}(i, multiHashWaitGroup, nextData)
	}
	multiHashWaitGroup.Wait()
	res := strings.Join(outArray, "")
	output <- res
	group.Done()
}


func CombineResults(in, out chan interface{}) {
	var slice []string
	for data := range in {
		slice = append(slice, data.(string))
	}
	sort.Strings(slice)
	res := strings.Join(slice, "_")
	out <- res
}