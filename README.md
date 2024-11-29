Simple and really fast tool for sync keys (for now only keys) from one redis cluster to another.

Uses CLUSTER GETKEYSINSLOT instead of scan, that very slow. Creates connection and worker for earch master in source/destination clusters

Batch size driving number of queries in pipe execution and lead to speed and memory consumption.

Prints every thousandth message for short

Help
```
Usage of ./redisclustercopy:
  -b int
        Batch size (default 1000)
  -cpuprofile file
        write cpu profile to file
  -d string
        Redis destination address
  -s string
        Redis source address
```

Example
```
$ time ./redisclusterprepare -d servername1:6771 -workers 10 --total 10000000 -ttl 600
2024/11/29 22:42:34 [1000:10000000] completed
2024/11/29 22:42:34 [2000:10000000] completed
2024/11/29 22:42:34 [3000:10000000] completed
2024/11/29 22:42:34 [4000:10000000] completed
2024/11/29 22:42:34 [5000:10000000] completed
2024/11/29 22:42:34 [6000:10000000] completed
2024/11/29 22:42:34 [7000:10000000] completed
2024/11/29 22:42:34 [8000:10000000] completed
2024/11/29 22:42:34 [9000:10000000] completed
...
2024/11/29 22:42:44 [10000000:10000000] completed
Done

real    0m10.990s
user    0m56.959s
sys     0m11.498s
```
```
$ time ./redisclustercopy -s servername1:6771 -d servername2:6781 -b 1000
2024/11/29 22:43:30 [1000:~10000000] Setled key "testkey229505" with ttl 544
2024/11/29 22:43:30 [2000:~10000000] Setled key "testkey5105600" with ttl 549
2024/11/29 22:43:30 [3000:~10000000] Setled key "testkey9327130" with ttl 554
2024/11/29 22:43:30 [4000:~10000000] Setled key "testkey7791309" with ttl 552
2024/11/29 22:43:30 [5000:~10000000] Setled key "testkey1411476" with ttl 546
2024/11/29 22:43:30 [6000:~10000000] Setled key "testkey9765445" with ttl 554
2024/11/29 22:43:30 [7000:~10000000] Setled key "testkey5006081" with ttl 549
2024/11/29 22:43:30 [8000:~10000000] Setled key "testkey6710114" with ttl 551
2024/11/29 22:43:30 [9000:~10000000] Setled key "testkey5373786" with ttl 550
2024/11/29 22:43:30 [10000:~10000000] Setled key "testkey4550012" with ttl 549
...
2024/11/29 22:43:49 [9999000:10000000] Setled key "testkey3405732" with ttl 528
Done

real    0m19.648s
user    1m26.479s
sys     0m11.343s
```