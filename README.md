# Dynamic Concurrent Pipeline in Go

A self-scaling **producer → workers → collector** pipeline written in pure Go (standard library only). A supervisor goroutine watches the job queue and grows or shrinks the worker pool on the fly, the whole system shuts down gracefully on `Ctrl+C`, and it can shut *itself* down if the error rate gets too high.

Built as the final project for an Operating Systems Lab course, to put concurrency, synchronization, resource management and process control into practice.

## Features

- **Dynamic worker pool**: the supervisor checks queue length every 500 ms and adds workers under load (up to a max) or removes them when idle (down to a min).
- **Stopping one worker at a time**: each worker has its own stop channel, so the supervisor can retire a single worker without touching the others. The retired worker finishes its current job before exiting.
- **Graceful shutdown**: on `SIGINT` the producer stops, workers finish their in-flight jobs, the collector drains every remaining result, and the process exits only after all goroutines are done.
- **Fault tolerance**: jobs fail at random; failures are logged and the pipeline keeps going.
- **Auto-shutdown on error rate** *(bonus)*: if errors in a 10 s window reach a threshold, the supervisor triggers the same graceful shutdown as `Ctrl+C`.
- **Priority queues** *(bonus, separate binary)*: high- and normal-priority lanes merged by a priority multiplexer, with scaling biased toward high-priority backlog.

## Architecture

```
                       ┌──────────────────────────────┐
                       │          Supervisor          │
                       │  • scales on len(jobCh)      │
                       │  • watches error counter     │
                       │  • cancels ctx if unhealthy  │
                       └──────┬───────────────▲───────┘
                  add / stop  │               │ atomic error count
                   (stopCh)   ▼               │
┌──────────┐  jobCh   ┌──────────────┐ resultCh ┌───────────┐
│ Producer ├─────────►│  Worker 1..N ├─────────►│ Collector │
└──────────┘ (buf 20) └──────────────┘ (buf 20) └───────────┘

   All stages share one context.Context, cancelled by Ctrl+C
   or by the supervisor's auto-shutdown.
```

In the priority variant, the producer writes to two lanes (`highIn`, `normIn`) and a **priority mux** forwards into `jobCh`, always taking from the high lane first when it has work.

### Key design decisions

| Decision | Why |
|---|---|
| Per-worker `stopCh` | A shared broadcast channel can't stop *one* worker. Closing a single worker's channel removes just that worker. |
| Non-blocking stop check + `shouldStop` flag | When `jobCh` is always ready, a plain `select` could keep picking jobs and never see the stop signal. The flag makes the worker exit right after its current job. |
| Buffered channels (capacity 20) | Lets a backlog build up so the supervisor has something to measure, and absorbs bursts. |
| `sync/atomic` error counter, reset per window | A cheap, race-free fixed-window error rate with no extra locking. |
| Single owner for each `close()` | Only the producer closes input channels, only the mux closes `jobCh` (priority build), and `resultCh` is closed only after every worker has exited, so nothing ever sends on a closed channel. |

More detail, plus annotated logs showing each feature working, is in **[docs/REPORT.md](docs/REPORT.md)**.

## Getting started

**Requirements:** Go 1.19+

```bash
git clone https://github.com/<your-username>/go-dynamic-pipeline.git
cd go-dynamic-pipeline
```

Run the core pipeline:

```bash
go run ./cmd/pipeline
```

Run the priority-queue variant:

```bash
go run ./cmd/pipeline-priority
```

Press **Ctrl+C** at any time to trigger a graceful shutdown. Either program may also stop by itself when the error-rate threshold is hit.

To build binaries instead:

```bash
go build -o bin/ ./cmd/...
```

## Configuration

The parameters are set in `run()` in each `main.go`:

| Parameter | `pipeline` | `pipeline-priority` |
|---|---|---|
| Min / max workers | 1 / 5 | 1 / 5 |
| Scale-up threshold (queue length) | > 3 | weighted backlog > 3, or any high-priority job waiting |
| Scale-down threshold (queue length) | < 1 | < 1 and no high-priority jobs waiting |
| Job production interval | 100 ms | 150 ms |
| High-priority share | n/a | 10 % |
| Simulated job duration | 100–500 ms | 100–500 ms |
| Simulated failure probability | 10 % | 20 % |
| Auto-shutdown | ≥ 10 errors in 10 s | ≥ 10 errors in 10 s |

## Sample output

```
Supervisor: added worker 1 (total workers: 1)
Producer created job 1
Collector: Worker 1 processed job 1 in 115.7ms
...
Supervisor: added worker 5 (total workers: 5)
Collector: Worker 4 failed job 166: simulated error on job 166
Supervisor: removed worker 5 (remaining workers: 4)
...
^CReceived interrupt signal, initiating graceful shutdown
Producer received shutdown signal, stopping job creation
Collector (drain): Worker 1 successfully processed job 5 in 195.1ms
Collector (drain): Worker 2 successfully processed job 6 in 225.2ms
Application shutdown complete
```

## Project structure

```
.
├── cmd/
│   ├── pipeline/            # core assignment: dynamic pool, graceful + auto shutdown
│   │   └── main.go
│   └── pipeline-priority/   # bonus: high/normal priority lanes with a priority mux
│       └── main.go
├── docs/
│   └── REPORT.md            # full design write-up and test logs
├── go.mod
└── README.md
```

## Concepts covered

Goroutines and channels · `select` · `context` cancellation · `sync.WaitGroup` · `sync/atomic` · `os/signal` · fan-out/fan-in · backpressure · channel ownership and close semantics

## License

[MIT](LICENSE)
