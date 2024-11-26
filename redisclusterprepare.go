package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

func worker1(client *redis.ClusterClient, ctx context.Context, num chan string, ttl *int, batchSize *int, out chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	pipeline := client.Pipeline()
	pipeExec := func() {
		_, err := pipeline.Exec(context.Background())
		if err != nil {
			fmt.Printf("Failed to set keys: %v\n", err)
		}
	}
	for {
		for i := 0; i < *batchSize; i++ {
			select {
			case n := <-num:
				pipeline.Set(context.Background(), "testkey"+n, "testval"+n, time.Duration(*ttl)*time.Second)
				out <- true
			case <-ctx.Done():
				pipeExec()
				return
			}
		}
		pipeExec()
	}
}

func preparePrinter(output chan bool, total *int, batchSize *int) {
	count := 0
	for {
		count++
		if count%(*batchSize) == 0 {
			log.Printf("[%d:%d] completed", count, *total)
		}
		<-output
	}
}

func main() {
	ttl := flag.Int("ttl", 7200, "ttl of records")
	totalRecords := flag.Int("total", 10000000, "total records")
	batchSize := flag.Int("batchSize", 1000, "batch size")
	workers := flag.Int("workers", 10, "number of workers")
	redisAddress := flag.String("d", "", "Redis source address")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{*redisAddress},
		Password: "", // no password set
	})
	ctx, cancel := context.WithCancel(context.Background())
	num := make(chan string, 10*(*workers)*(*batchSize))
	out := make(chan bool, 10*(*workers)*(*batchSize))
	wg := &sync.WaitGroup{}
	go preparePrinter(out, totalRecords, batchSize)
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker1(rdb, ctx, num, ttl, batchSize, out, wg)
	}
	for i := 0; i < *totalRecords; i++ {
		num <- strconv.Itoa(i)
	}
	for len(num) > 0 {
		time.Sleep(1 * time.Second)
	}
	cancel()
	wg.Wait()
	fmt.Println("Done")
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
