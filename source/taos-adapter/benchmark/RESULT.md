# benchmark

```shell
root@vm96 /home/taosadapter/taosadapter/benchmark (master)$ go test -bench=.
goos: linux
goarch: amd64
pkg: github.com/huskar-t/taosadapter_demo/benchmark
cpu: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz
BenchmarkRoute-16    	 1238919	       988.4 ns/op

BenchmarkRoute-16    	 1000000	      1027 ns/op

BenchmarkRoute-16    	 1203561	      1025 ns/op

BenchmarkRoute-16    	 1224735	      1031 ns/op

BenchmarkRoute-16    	 1000000	      1124 ns/op
```

# benchmark with memory info

```shell
root@vm96 /home/taosadapter/taosadapter/benchmark (master)$ go test -bench=. -benchmem
goos: linux
goarch: amd64
pkg: github.com/huskar-t/taosadapter_demo/benchmark
cpu: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz
BenchmarkRoute-16    	 1000000	      1003 ns/op	     448 B/op	       3 allocs/op

BenchmarkRoute-16    	 1000000	      1120 ns/op	     448 B/op	       3 allocs/op

BenchmarkRoute-16    	 1331481	      1071 ns/op	     448 B/op	       3 allocs/op

BenchmarkRoute-16    	 1000000	      1088 ns/op	     448 B/op	       3 allocs/op

BenchmarkRoute-16    	 1000000	      1036 ns/op	     448 B/op	       3 allocs/op
```
