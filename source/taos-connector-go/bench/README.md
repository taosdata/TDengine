##Usage
```shell
go test -bench=. -benchmem -memprofile memprofile.out -cpuprofile profile.out
```
```shell
go tool pprof memprofile.out
```
```shell
go tool pprof profile.out
```
