# Map Reduce system

## Build
```
cd main
go build -race -buildmode=plugin ../mrapps/wc.go
```

## Run
### 1. Run Coordinator
```
go run -race mrcoordinator.go pg-*.txt
```

### 2. Run Workers
```
go run -race mrworker.go wc.so
```

## Note 
Can lauch mutiples worker by just repeating the command for running workers for multiple time,
In realworld each worker is typically run in a separate machine

Can enable log by put VERBOSE=1 before the commands, for example:
```
VERBOSE=1 go run -race mrcoordinator.go pg-*.txt
```
