#!/bin/bash
set -e

# Configuration
HOST="127.0.0.1"
PORT="6379"
RUSTBUCKET_BIN="./target/release/rustbucket"
DURATION=30
CLIENTS=50
PIPELINE=16

# Function to start RustBucket
start_server() {
    echo "Starting RustBucket..."
    stop_server # ensure old is dead
    rm -f $RUSTBUCKET_BIN # force remove old binary
    cargo build --release
    ls -li $RUSTBUCKET_BIN
    $RUSTBUCKET_BIN > server.log 2>&1 &
    SERVER_PID=$!
    echo "RustBucket started with PID $SERVER_PID"
    sleep 2 # Wait for startup
}

# Function to stop RustBucket
stop_server() {
    echo "Stopping RustBucket..."
    pkill -f "target/release/rustbucket" || true
    echo "RustBucket stopped."
}

# Function to monitor memory
monitor_memory() {
    echo "Monitoring memory usage for PID $SERVER_PID..."
    echo "Time(s), RSS(KB)" > memory_log.csv
    end=$((SECONDS+$DURATION+10))
    while [ $SECONDS -lt $end ]; do
        if ps -p $SERVER_PID > /dev/null; then
            rss=$(ps -o rss= -p $SERVER_PID | tr -d ' ')
            echo "$SECONDS, $rss" >> memory_log.csv
        else
            break
        fi
        sleep 1
    done &
    MONITOR_PID=$!
}

# Main Benchmark Suite
# Reference Redis Configuration
REDIS_PORT="6380"
REDIS_BIN="redis-server"

start_reference_redis() {
    echo "Starting Reference Redis on port $REDIS_PORT..."
    $REDIS_BIN --port $REDIS_PORT --daemonize yes
    sleep 1
}

stop_reference_redis() {
    echo "Stopping Reference Redis..."
    redis-cli -p $REDIS_PORT shutdown
}

run_benchmarks() {
    TARGET_PORT=$1
    LABEL=$2
    
    echo "========================================"
    echo "Running Benchmarks against $LABEL (Port $TARGET_PORT)"
    echo "========================================"

    # 1. PING (Latency Baseline)
    echo "1. PING (Baseline Latency)"
    redis-benchmark -h $HOST -p $TARGET_PORT -t ping -c $CLIENTS -n 100000 -q

    # 2. SET/GET (Throughput)
    echo "2. SET/GET (Throughput)"
    redis-benchmark -h $HOST -p $TARGET_PORT -t set,get -c $CLIENTS -n 100000 -q

    # 3. SET/GET with Pipelining (High Throughput)
    echo "3. SET/GET Pipelined (P=$PIPELINE)"
    redis-benchmark -h $HOST -p $TARGET_PORT -t set,get -c $CLIENTS -n 100000 -P $PIPELINE -q

    # 4. LPUSH (List Performance)
    echo "4. LPUSH (List Performance)"
    redis-benchmark -h $HOST -p $TARGET_PORT -t lpush -c $CLIENTS -n 100000 -q
}

# Execution
# 1. Test RustBucket
start_server
monitor_memory
run_benchmarks $PORT "RustBucket"
kill $MONITOR_PID || true
stop_server

# 2. Test Reference Redis
start_reference_redis
run_benchmarks $REDIS_PORT "Redis (Reference)"
stop_reference_redis

echo "========================================"
echo "Memory usage logged to memory_log.csv"
echo "Check for leaks: Ensure RSS stabilizes and doesn't grow linearly indefinitely."
echo "========================================"
