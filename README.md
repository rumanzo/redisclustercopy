Help
Usage of ./redisclustercopy:
  -b int
        Batch size (default 1000)
  -cpuprofile file
        write cpu profile to file
  -d string
        Redis destination address
  -s string
        Redis source address
  -w int
        Number of workers (default 10)

Need to define source/destination address (at least one), and batch size for redis pipe and num workers for set keys.
