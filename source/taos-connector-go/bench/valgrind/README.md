##Usage
```shell
go build -o t main.go
valgrind --log-file="result.txt" --leak-check=full ./t
```