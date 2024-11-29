package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

type KeyValue struct {
	Key   *string
	Value *string
	TTL   *time.Duration
}

func worker(clientDestination *redis.Client, ctx context.Context, keys chan *KeyValue, output chan string, batchSize int, wg *sync.WaitGroup) {
	defer wg.Done()
	pipeline := clientDestination.Pipeline()
	pipeExec := func() {
		_, err := pipeline.Exec(context.Background())
		if err != nil {
			output <- fmt.Sprintf("Failed to set keys: %v\n", err)
		}
	}
	for {
		for i := 0; i < batchSize; i++ {
			if kv, ok := <-keys; ok {
				pipeline.Set(context.Background(), *kv.Key, *kv.Value, *kv.TTL)
				output <- fmt.Sprintf("Setled key %q with ttl %d\n", *kv.Key, int((*kv.TTL).Seconds()))
			} else {
				pipeExec()
				return
			}
		}
		pipeExec()
	}
}

func printer(output chan string, total int64, batchSize *int) {
	count := 0
	msg := ""
	for {
		count++
		if count%(*batchSize) == 0 {
			msg = <-output
			log.Printf("[%d:~%d] %v", count, total, msg)
		} else {
			<-output
		}
	}
}

func main() {
	redisSourceAddress := flag.String("s", "", "Redis source address")
	redisDestinationAddress := flag.String("d", "", "Redis destination address")
	batchSize := flag.Int("b", 1000, "Batch size")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
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
	if redisSourceAddress == nil || *redisSourceAddress == "" {
		fmt.Println("Please specify a redis source address")
		os.Exit(1)
	}
	rdb1 := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{*redisSourceAddress},
		ReadOnly: true,
		Password: "", // no password set
	})
	rdb2 := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{*redisDestinationAddress},
		Password: "", // no password set
	})

	ctx, cancel := context.WithCancel(context.Background())
	dbsize, err := rdb1.DBSize(ctx).Result()
	if err != nil {
		fmt.Printf("Error getting DB size: %v\n", err)
	}
	output := make(chan string, 10*(*batchSize))
	wg := &sync.WaitGroup{}
	go printer(output, dbsize, batchSize)
	slots, err := rdb1.ClusterSlots(context.Background()).Result()
	if err != nil {
		fmt.Printf("Error getting cluster slots: %v\n", err)
	}

	destinationClients := map[*redis.Client]chan *KeyValue{}
	mut := &sync.RWMutex{}

	err = rdb1.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		masterNodeID, err := master.Do(ctx, "CLUSTER", "MYID").Result()
		if err != nil {
			fmt.Printf("Error getting master node id: %v\n", err)
		}
		batchNum := 0
		pipe := master.Pipeline()
		cmdsVal := map[string]*redis.StringCmd{}
		cmdsTTL := map[string]*redis.DurationCmd{}
		batchExec := func() {
			_, err = pipe.Exec(ctx)
			if err != nil && err != redis.Nil {
				fmt.Printf("Failed to exec pipe: %v\n", err)
			}
			for k, v := range cmdsVal {
				val, err := v.Result()
				if err != nil {
					output <- fmt.Sprintf("Failed to get key %q (probably key not exists, expired): %v\n", k, err)
					continue
				}
				ttl, err := cmdsTTL[k].Result()
				if err != nil {
					output <- fmt.Sprintf("Failed to get ttl for key %q: %v\n", k, err)
					continue
				}
				mut.RLock()
				keyClient, _ := rdb2.MasterForKey(ctx, k)
				mut.RUnlock()
				clientKeyValChan, exists := destinationClients[keyClient]
				if exists {
					clientKeyValChan <- &KeyValue{Key: &k, Value: &val, TTL: &ttl}
				} else {
					mut.Lock()
					destinationClients[keyClient] = make(chan *KeyValue, 10*(*batchSize))
					mut.Unlock()
					wg.Add(1)
					go worker(keyClient, ctx, destinationClients[keyClient], output, *batchSize, wg)
				}
			}
			cmdsVal, cmdsTTL = map[string]*redis.StringCmd{}, map[string]*redis.DurationCmd{}
			batchNum = 0
		}

		for _, clusterSlot := range slots {
			for _, node := range clusterSlot.Nodes {
				if node.ID == masterNodeID {
					for i := clusterSlot.Start; i < clusterSlot.End+1; i++ {
						keysInSlot, _ := master.ClusterCountKeysInSlot(ctx, i).Result()
						keys, err := master.ClusterGetKeysInSlot(ctx, i, int(keysInSlot)).Result()
						if err != nil {
							fmt.Printf("Error getting keys: %v\n", err)
						}
						for _, key := range keys {
							batchNum++
							cmdsVal[key], cmdsTTL[key] = pipe.Get(ctx, key), pipe.TTL(ctx, key)
							if batchNum >= *batchSize {
								batchExec()
							}
						}
					}
				}
			}
		}
		batchExec()
		return nil
	})
	for _, ch := range destinationClients {
		close(ch)
	}
	cancel()
	if err != nil {
		fmt.Println(err)
	}
	wg.Wait()
	fmt.Println("Done")
}
