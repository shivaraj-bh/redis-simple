# WIP: redis-simple
This haskell project packages the following:
- **redis-simple** (A library to send raw commands to redis over sockets)
- **redis-batching-bench** (An executable to benchmark **redis-simple** with batching[^batching])

[^batching]: Batching here refers to reducing the number of system calls by sending multiple redis commands at once. 

## Run the benchmark
```
nix run github:shivaraj-bh/redis-simple
```
## Development
```
git clone https://github.com/shivaraj-bh/redis-simple.git
cd redis-simple
nix develop
```
## Benchmark configuration
You can play around with the following knobs in the `app/Main.hs` file:
- **clients** (Number of clients to spawn, `default: 5`)
- **requests** (Number of requests to send per client, `default: 2`)
- **bufferSize** (Size of the buffer to use for batching, `default:10`)
