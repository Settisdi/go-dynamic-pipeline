# Dynamic Concurrent Pipeline in Go

This project implements a **three‑stage concurrent pipeline** in Go that demonstrates key operating system concepts: concurrency, synchronization, resource management, and process control.  It simulates a producer–worker–collector architecture where a pool of workers processes jobs and automatically adapts to changing workloads.  The application also supports graceful shutdown on both OS interrupts and excessive error rates.

## Design Explanation

### Components

1. **Jobs and Results**
   
   - A *job* is represented by a simple `Job` struct containing a unique integer ID.
   - A *result* is represented by a `Result` struct with fields for the job ID, the worker ID that processed it, processing duration, and an optional error.  Errors are simulated randomly to test the error handling path.

2. **Producer**

   - A single goroutine (`producer`) periodically creates new jobs and pushes them into a buffered job queue.  A ticker produces roughly five jobs per second, allowing the queue to grow under load and exercise the scaling logic.
   - The producer listens to a context; when cancellation is signalled (via Ctrl+C or a supervisor shutdown), it stops creating jobs and closes the job channel so that workers know there will be no more work.

3. **Workers**

   - Workers are encapsulated by a `worker` type containing an ID and a `stopCh`.  Each worker runs in its own goroutine (`startWorker`).
   - Workers pull jobs from the job channel, simulate work by sleeping for a random duration between 100–500 ms, and with a configurable probability (`0.1` in this implementation) simulate an error.  The outcome (success or error) is sent to the result channel.
   - To gracefully stop a single worker, the supervisor closes that worker’s `stopCh`.  The worker periodically checks this channel non‑blocking and sets a `shouldStop` flag.  After finishing its current job it sees this flag and exits cleanly.  This mechanism avoids race conditions where the worker might exit before finishing a job or ignore the stop signal due to a continuously ready job channel.

4. **Collector**

   - A dedicated goroutine (`collector`) reads results from the result channel and logs them.  It increments an error counter when failures occur, allowing the supervisor to monitor the error rate.
   - When the context is cancelled, the collector continues draining the result channel until it is closed to ensure no result is lost, then exits.

5. **Supervisor**

   - The supervisor runs in its own goroutine and is responsible for **dynamic scaling** and **health monitoring**:
     
     - Every 500 ms it checks the length of the job queue and decides whether to add or remove workers.
       - **Scale up**: If the queue length exceeds a `scaleUpThresh` and the number of workers is below `maxWorkers` , a new worker is created.  Each worker is assigned a unique ID and its `stopCh` is tracked for later removal.
       - **Scale down**: If the queue length falls below `scaleDownThresh`  and the number of workers is above `minWorkers` , the supervisor removes the most recently added worker by closing its `stopCh`.  That worker finishes its current job and exits.
     - In addition, every `errorWindow` (10 s) it reads the atomic error counter.  If the count exceeds `errorThreshold` (10 errors in the window), the supervisor logs an error and triggers a global shutdown by cancelling the context.  The error counter is then reset for the next window.  This implements auto‑shutdown behaviour.
   - On cancellation, the supervisor closes the `stopCh` of all remaining workers to ensure they exit.

6. **Graceful Shutdown**

   - The main `run` function creates a root context with `context.WithCancel`.  When an OS interrupt (`Ctrl+C`) is received, the context is cancelled.  The supervisor can also cancel the context when the error rate exceeds the threshold.
   - When the context is cancelled:
     - The producer stops creating jobs and closes the job channel.
     - The supervisor closes all workers’ stop channels.
     - Workers finish their current jobs and exit.
     - The collector drains any remaining results and exits.
     - The main goroutine waits for the producer and all workers to finish, closes the result channel, and waits for the collector to finish before printing a final shutdown message.

### Key Design Decisions

- **Per‑worker stop channels**:  A single broadcast channel cannot stop individual workers without stopping all of them.  By giving each worker its own `stopCh` the supervisor can remove only one worker while others continue processing jobs.
- **Non‑blocking stop detection**:  Workers occasionally check `stopCh` in a non‑blocking manner.  Without this, a continuously ready job channel could starve the stop signal.  After receiving a stop request, a `shouldStop` flag ensures the worker exits only after finishing the current job.
- **Buffered channels**:  The job and result channels have a capacity (20) to allow a backlog and smooth out bursts.  A full job queue triggers scaling and prevents the producer from blocking too early.
- **Atomic error counter**:  The supervisor monitors a simple atomic counter rather than scanning logs.  The counter is reset each sampling window to implement a moving error window without complex data structures.

## How to Run

### Prerequisites

- Go 1.19 or later installed (the code uses standard packages only).

### Building and Running

1. Open a terminal and navigate to the project root.
2. Build the application:

   ```sh
   go build -o pipeline ./cmd/pipeline
   ```

   or run it directly without building:

   ```sh
   go run ./cmd/pipeline
   ```

3. The program will start producing jobs, spawning workers, and logging activity.  To stop it manually, press **Ctrl+C**.  The application will perform a graceful shutdown.

### Command‑line Output

The program writes informative logs to standard output.  Each line is prefixed with a timestamp for readability.  Key log messages include:

- `Producer created job …`: the producer generated a new job.
- `Supervisor: added/removed worker …`: the supervisor scaled the worker pool up or down.
- `Collector: Worker … processed job …`: a job was completed successfully along with its duration.
- `Collector: Worker … failed job …`: a simulated error occurred (does not crash the program).
- `Supervisor: error threshold exceeded`: the bonus logic triggered an automatic shutdown.
- `Received interrupt signal, initiating graceful shutdown`: the user pressed Ctrl+C.

## Testing & Analysis

The following sample log excerpts demonstrate that each required feature works correctly.  These were produced by running the program for about 20 seconds and then pressing Ctrl+C.

### Dynamic Worker Pool

As the job queue grows, the supervisor adds workers.  When the queue shrinks, it removes workers.  The application starts with one worker (the minimum), then scales up to five workers when the queue length exceeds the threshold, and later scales back down when idle.

```
…
2025/08/22 01:26:21 Producer created job 160
2025/08/22 01:26:21 Supervisor: added worker 5 (total workers: 5) <---- added worker
2025/08/22 01:26:21 Producer created job 161
2025/08/22 01:26:21 Collector: Worker 2 processed job 155 in 176.734875ms
2025/08/22 01:26:21 Collector: Worker 3 processed job 156 in 158.139ms
2025/08/22 01:26:21 Producer created job 162
2025/08/22 01:26:21 Collector: Worker 1 processed job 152 in 439.556958ms
2025/08/22 01:26:21 Collector: Worker 4 processed job 153 in 426.136167ms
2025/08/22 01:26:21 Producer created job 163
2025/08/22 01:26:21 Producer created job 164
2025/08/22 01:26:21 Collector: Worker 3 processed job 159 in 254.587125ms
2025/08/22 01:26:21 Collector: Worker 5 processed job 157 in 430.081ms
2025/08/22 01:26:21 Collector: Worker 2 processed job 158 in 327.932791ms
2025/08/22 01:26:21 Collector: Worker 1 processed job 160 in 250.284541ms
2025/08/22 01:26:21 Producer created job 165
2025/08/22 01:26:21 Supervisor: removed worker 5 (remaining workers: 4) <--- removed worker
2025/08/22 01:26:21 Collector: Worker 4 processed job 161 in 291.145584ms
2025/08/22 01:26:21 Collector: Worker 3 processed job 162 in 172.071583ms
2025/08/22 01:26:21 Collector: Worker 2 processed job 164 in 151.023583ms
2025/08/22 01:26:21 Producer created job 166
2025/08/22 01:26:21 Collector: Worker 5 processed job 163 in 256.11975ms
2025/08/22 01:26:21 Producer created job 167
2025/08/22 01:26:21 Collector: Worker 4 failed job 166: simulated error on job 166
2025/08/22 01:26:21 Producer created job 168
2025/08/22 01:26:22 Producer created job 169
2025/08/22 01:26:22 Collector: Worker 1 processed job 165 in 404.510458ms
2025/08/22 01:26:22 Collector: Worker 2 failed job 168: simulated error on job 168
2025/08/22 01:26:22 Collector: Worker 3 processed job 167 in 290.172125ms
2025/08/22 01:26:22 Producer created job 170
2025/08/22 01:26:22 Supervisor: removed worker 4 (remaining workers: 3) <--- added worker
2025/08/22 01:26:22 Collector: Worker 4 failed job 169: simulated error on job 169
…
```


### Graceful Shutdown

When the user presses **Ctrl+C**, the program stops generating new jobs and waits for all workers to finish their current jobs.  The collector continues draining results until all workers exit, then the program terminates cleanly.

```
…
2025/08/22 01:29:59 Supervisor: added worker 1 (total workers: 1)
2025/08/22 01:29:59 Producer created job 1
2025/08/22 01:29:59 Producer created job 2
2025/08/22 01:29:59 Producer created job 3
2025/08/22 01:29:59 Collector: Worker 1 failed job 1: simulated error on job 1
2025/08/22 01:29:59 Producer created job 4
2025/08/22 01:30:00 Producer created job 5
2025/08/22 01:30:00 Collector: Worker 1 processed job 2 in 140.100917ms
2025/08/22 01:30:00 Producer created job 6
2025/08/22 01:30:00 Collector: Worker 1 processed job 3 in 100.561458ms
2025/08/22 01:30:00 Producer created job 7
2025/08/22 01:30:00 Producer created job 8
2025/08/22 01:30:00 Producer created job 9
2025/08/22 01:30:00 Collector: Worker 1 processed job 4 in 371.189917ms
2025/08/22 01:30:00 Producer created job 10
2025/08/22 01:30:00 Supervisor: added worker 2 (total workers: 2)
^C2025/08/22 01:30:00 Received interrupt signal, initiating graceful shutdown <---- CTRL + C
2025/08/22 01:30:00 Producer received shutdown signal, stopping job creation
2025/08/22 01:30:00 Collector (drain): Worker 1 successfully processed job 5 in 195.122625ms
2025/08/22 01:30:00 Collector (drain): Worker 2 successfully processed job 6 in 225.158875ms
2025/08/22 01:30:00 Application shutdown complete

```

The order of messages illustrates that:

1. Ctrl+C triggers a shutdown message.
2. The producer stops and closes the job channel.
3. The supervisor signals every worker to stop.
4. Workers finish their current jobs and exit; the collector drains the remaining results.
5. The program exits only after all goroutines finish, reporting `Application shutdown complete`.

### Error Handling

Workers randomly fail one out of every five jobs (10 % probability).  Failed jobs are logged, but the system continues processing other jobs.  The sample logs show both successful and failed job processing:

```
...
2025/08/22 01:26:12 Producer created job 75
2025/08/22 01:26:12 Collector: Worker 1 processed job 71 in 361.192667ms
2025/08/22 01:26:12 Collector: Worker 3 processed job 74 in 197.088958ms
2025/08/22 01:26:12 Producer created job 76
2025/08/22 01:26:12 Collector: Worker 4 failed job 70: simulated error on job 70 <----
2025/08/22 01:26:12 Producer created job 77
2025/08/22 01:26:12 Collector: Worker 1 processed job 75 in 138.091ms
2025/08/22 01:26:12 Producer created job 78
2025/08/22 01:26:12 Collector: Worker 2 processed job 73 in 465.71525ms
2025/08/22 01:26:13 Producer created job 79
2025/08/22 01:26:13 Collector: Worker 1 processed job 78 in 133.113458ms
...
```

The program does not crash on errors; instead, it logs the failure and proceeds.

### Bonus: Auto‑Shutdown on Critical Error Rate

Every 10 seconds the supervisor checks whether at least 10 errors occurred in that interval.  If the count meets or exceeds the threshold, the supervisor logs a message and initiates a full shutdown just like Ctrl+C. The following log excerpt shows an automatic shutdown due to a high error rate:

```
…
2025/08/22 01:26:25 Supervisor: error threshold exceeded (13 errors), initiating shutdown
2025/08/22 01:26:25 Producer received shutdown signal, stopping job creation
2025/08/22 01:26:25 Collector (drain): Worker 2 successfully processed job 192 in 463.153208ms
2025/08/22 01:26:25 Collector (drain): Worker 4 successfully processed job 197 in 222.149792ms
2025/08/22 01:26:25 Collector (drain): Worker 3 successfully processed job 198 in 427.713083ms
2025/08/22 01:26:25 Collector (drain): Worker 1 successfully processed job 199 in 484.151542ms
2025/08/22 01:26:25 Application shutdown complete
```

In this run the error counter hit 13 within a 10‑second window.  The supervisor proactively cancelled the context, and the application shut down gracefully.

**Full log output from the run used above:**

```
2025/08/22 01:26:05 Supervisor: added worker 1 (total workers: 1)
2025/08/22 01:26:05 Producer created job 1
2025/08/22 01:26:05 Producer created job 2
2025/08/22 01:26:05 Producer created job 3
2025/08/22 01:26:05 Producer created job 4
2025/08/22 01:26:05 Collector: Worker 1 processed job 1 in 329.121333ms
2025/08/22 01:26:05 Producer created job 5
2025/08/22 01:26:05 Collector: Worker 1 processed job 2 in 124.096042ms
2025/08/22 01:26:05 Producer created job 6
2025/08/22 01:26:05 Producer created job 7
2025/08/22 01:26:05 Producer created job 8
2025/08/22 01:26:06 Collector: Worker 1 processed job 3 in 285.118291ms
2025/08/22 01:26:06 Producer created job 9
2025/08/22 01:26:06 Producer created job 10
2025/08/22 01:26:06 Supervisor: added worker 2 (total workers: 2)
2025/08/22 01:26:06 Producer created job 11
2025/08/22 01:26:06 Producer created job 12
2025/08/22 01:26:06 Collector: Worker 2 processed job 5 in 228.915041ms
2025/08/22 01:26:06 Collector: Worker 1 processed job 4 in 441.086209ms
2025/08/22 01:26:06 Producer created job 13
2025/08/22 01:26:06 Producer created job 14
2025/08/22 01:26:06 Producer created job 15
2025/08/22 01:26:06 Supervisor: added worker 3 (total workers: 3)
2025/08/22 01:26:06 Collector: Worker 2 processed job 6 in 361.607167ms
2025/08/22 01:26:06 Producer created job 16
2025/08/22 01:26:06 Producer created job 17
2025/08/22 01:26:06 Collector: Worker 1 processed job 7 in 425.768417ms
2025/08/22 01:26:06 Producer created job 18
2025/08/22 01:26:07 Collector: Worker 3 processed job 8 in 322.6815ms
2025/08/22 01:26:07 Collector: Worker 2 processed job 9 in 233.053458ms
2025/08/22 01:26:07 Producer created job 19
2025/08/22 01:26:07 Collector: Worker 3 processed job 11 in 125.149792ms
2025/08/22 01:26:07 Collector: Worker 1 processed job 10 in 256.069875ms
2025/08/22 01:26:07 Producer created job 20
2025/08/22 01:26:07 Supervisor: added worker 4 (total workers: 4)
2025/08/22 01:26:07 Producer created job 21
2025/08/22 01:26:07 Collector: Worker 1 processed job 14 in 152.993875ms
2025/08/22 01:26:07 Producer created job 22
2025/08/22 01:26:07 Collector: Worker 2 processed job 12 in 432.746ms
2025/08/22 01:26:07 Producer created job 23
2025/08/22 01:26:07 Producer created job 24
2025/08/22 01:26:07 Collector: Worker 3 processed job 13 in 458.407792ms
2025/08/22 01:26:07 Collector: Worker 4 processed job 15 in 430.347875ms
2025/08/22 01:26:07 Collector: Worker 2 processed job 17 in 219.689375ms
2025/08/22 01:26:07 Producer created job 25
2025/08/22 01:26:07 Supervisor: added worker 5 (total workers: 5)
2025/08/22 01:26:07 Collector: Worker 3 processed job 18 in 103.069333ms
2025/08/22 01:26:07 Collector: Worker 1 processed job 16 in 461.159667ms
2025/08/22 01:26:07 Producer created job 26
2025/08/22 01:26:07 Collector: Worker 5 processed job 21 in 129.16625ms
2025/08/22 01:26:07 Collector: Worker 3 processed job 22 in 163.433333ms
2025/08/22 01:26:07 Producer created job 27
2025/08/22 01:26:07 Collector: Worker 2 processed job 20 in 226.011084ms
2025/08/22 01:26:07 Producer created job 28
2025/08/22 01:26:07 Collector: Worker 1 processed job 23 in 230.702917ms
2025/08/22 01:26:08 Collector: Worker 2 failed job 26: simulated error on job 26
2025/08/22 01:26:08 Collector: Worker 5 processed job 24 in 263.573084ms
2025/08/22 01:26:08 Producer created job 29
2025/08/22 01:26:08 Collector: Worker 4 processed job 19 in 480.073459ms
2025/08/22 01:26:08 Collector: Worker 1 processed job 27 in 116.081083ms
2025/08/22 01:26:08 Collector: Worker 3 processed job 25 in 264.494125ms
2025/08/22 01:26:08 Producer created job 30
2025/08/22 01:26:08 Supervisor: removed worker 5 (remaining workers: 4)
2025/08/22 01:26:08 Collector: Worker 2 processed job 28 in 192.605ms
2025/08/22 01:26:08 Producer created job 31
2025/08/22 01:26:08 Producer created job 32
2025/08/22 01:26:08 Collector: Worker 4 processed job 30 in 238.641625ms
2025/08/22 01:26:08 Collector: Worker 5 processed job 29 in 366.122583ms
2025/08/22 01:26:08 Producer created job 33
2025/08/22 01:26:08 Collector: Worker 3 processed job 32 in 142.009084ms
2025/08/22 01:26:08 Collector: Worker 1 processed job 31 in 281.472583ms
2025/08/22 01:26:08 Producer created job 34
2025/08/22 01:26:08 Producer created job 35
2025/08/22 01:26:08 Supervisor: removed worker 4 (remaining workers: 3)
2025/08/22 01:26:08 Collector: Worker 4 processed job 34 in 200.133375ms
2025/08/22 01:26:08 Producer created job 36
2025/08/22 01:26:08 Collector: Worker 2 processed job 33 in 330.79ms
2025/08/22 01:26:08 Producer created job 37
2025/08/22 01:26:08 Collector: Worker 3 processed job 35 in 248.05875ms
2025/08/22 01:26:08 Producer created job 38
2025/08/22 01:26:09 Producer created job 39
2025/08/22 01:26:09 Producer created job 40
2025/08/22 01:26:09 Producer created job 41
2025/08/22 01:26:09 Collector: Worker 1 processed job 36 in 499.6155ms
2025/08/22 01:26:09 Collector: Worker 2 processed job 37 in 492.248166ms
2025/08/22 01:26:09 Producer created job 42
2025/08/22 01:26:09 Collector: Worker 1 processed job 39 in 157.209666ms
2025/08/22 01:26:09 Collector: Worker 3 processed job 38 in 488.087ms
2025/08/22 01:26:09 Producer created job 43
2025/08/22 01:26:09 Producer created job 44
2025/08/22 01:26:09 Collector: Worker 1 processed job 41 in 211.145834ms
2025/08/22 01:26:09 Producer created job 45
2025/08/22 01:26:09 Producer created job 46
2025/08/22 01:26:09 Collector: Worker 3 processed job 42 in 333.157875ms
2025/08/22 01:26:09 Collector: Worker 2 processed job 40 in 484.150625ms
2025/08/22 01:26:09 Producer created job 47
2025/08/22 01:26:09 Producer created job 48
2025/08/22 01:26:10 Collector: Worker 2 processed job 45 in 155.101875ms
2025/08/22 01:26:10 Producer created job 49
2025/08/22 01:26:10 Collector: Worker 1 processed job 43 in 499.156916ms
2025/08/22 01:26:10 Producer created job 50
2025/08/22 01:26:10 Producer created job 51
2025/08/22 01:26:10 Collector: Worker 3 processed job 44 in 487.002083ms
2025/08/22 01:26:10 Collector: Worker 2 processed job 46 in 286.076209ms
2025/08/22 01:26:10 Producer created job 52
2025/08/22 01:26:10 Collector: Worker 1 processed job 47 in 285.454208ms
2025/08/22 01:26:10 Producer created job 53
2025/08/22 01:26:10 Collector: Worker 3 processed job 48 in 257.682708ms
2025/08/22 01:26:10 Collector: Worker 1 processed job 50 in 128.070125ms
2025/08/22 01:26:10 Producer created job 54
2025/08/22 01:26:10 Producer created job 55
2025/08/22 01:26:10 Collector: Worker 2 processed job 49 in 433.270834ms
2025/08/22 01:26:10 Collector: Worker 1 processed job 52 in 208.499209ms
2025/08/22 01:26:10 Producer created job 56
2025/08/22 01:26:10 Collector: Worker 3 processed job 51 in 281.647542ms
2025/08/22 01:26:10 Producer created job 57
2025/08/22 01:26:10 Collector: Worker 1 processed job 54 in 139.465166ms
2025/08/22 01:26:10 Producer created job 58
2025/08/22 01:26:11 Collector: Worker 1 failed job 56: simulated error on job 56
2025/08/22 01:26:11 Producer created job 59
2025/08/22 01:26:11 Producer created job 60
2025/08/22 01:26:11 Collector: Worker 2 processed job 53 in 461.904334ms
2025/08/22 01:26:11 Producer created job 61
2025/08/22 01:26:11 Collector: Worker 3 processed job 55 in 470.089959ms
2025/08/22 01:26:11 Producer created job 62
2025/08/22 01:26:11 Collector: Worker 2 processed job 58 in 239.451875ms
2025/08/22 01:26:11 Producer created job 63
2025/08/22 01:26:11 Collector: Worker 1 processed job 57 in 486.082584ms
2025/08/22 01:26:11 Producer created job 64
2025/08/22 01:26:11 Producer created job 65
2025/08/22 01:26:11 Supervisor: added worker 4 (total workers: 4)
2025/08/22 01:26:11 Collector: Worker 3 processed job 59 in 422.1575ms
2025/08/22 01:26:11 Collector: Worker 2 processed job 60 in 300.63375ms
2025/08/22 01:26:11 Collector: Worker 1 failed job 61: simulated error on job 61
2025/08/22 01:26:11 Producer created job 66
2025/08/22 01:26:11 Producer created job 67
2025/08/22 01:26:11 Collector: Worker 4 processed job 62 in 202.755292ms
2025/08/22 01:26:11 Collector: Worker 2 processed job 64 in 205.324833ms
2025/08/22 01:26:11 Collector: Worker 1 processed job 65 in 189.340208ms
2025/08/22 01:26:11 Producer created job 68
2025/08/22 01:26:12 Collector: Worker 3 processed job 63 in 335.12125ms
2025/08/22 01:26:12 Producer created job 69
2025/08/22 01:26:12 Producer created job 70
2025/08/22 01:26:12 Producer created job 71
2025/08/22 01:26:12 Collector: Worker 4 processed job 66 in 426.092958ms
2025/08/22 01:26:12 Producer created job 72
2025/08/22 01:26:12 Collector: Worker 1 processed job 68 in 419.012042ms
2025/08/22 01:26:12 Collector: Worker 2 processed job 67 in 475.946042ms
2025/08/22 01:26:12 Producer created job 73
2025/08/22 01:26:12 Collector: Worker 2 processed job 72 in 104.534709ms
2025/08/22 01:26:12 Collector: Worker 3 failed job 69: simulated error on job 69
2025/08/22 01:26:12 Producer created job 74
2025/08/22 01:26:12 Producer created job 75
2025/08/22 01:26:12 Collector: Worker 1 processed job 71 in 361.192667ms
2025/08/22 01:26:12 Collector: Worker 3 processed job 74 in 197.088958ms
2025/08/22 01:26:12 Producer created job 76
2025/08/22 01:26:12 Collector: Worker 4 failed job 70: simulated error on job 70
2025/08/22 01:26:12 Producer created job 77
2025/08/22 01:26:12 Collector: Worker 1 processed job 75 in 138.091ms
2025/08/22 01:26:12 Producer created job 78
2025/08/22 01:26:12 Collector: Worker 2 processed job 73 in 465.71525ms
2025/08/22 01:26:13 Producer created job 79
2025/08/22 01:26:13 Collector: Worker 1 processed job 78 in 133.113458ms
2025/08/22 01:26:13 Producer created job 80
2025/08/22 01:26:13 Supervisor: removed worker 4 (remaining workers: 3)
2025/08/22 01:26:13 Collector: Worker 2 processed job 79 in 157.74925ms
2025/08/22 01:26:13 Collector: Worker 3 processed job 76 in 489.621541ms
2025/08/22 01:26:13 Producer created job 81
2025/08/22 01:26:13 Collector: Worker 4 processed job 77 in 405.929375ms
2025/08/22 01:26:13 Producer created job 82
2025/08/22 01:26:13 Collector: Worker 2 processed job 81 in 170.158834ms
2025/08/22 01:26:13 Producer created job 83
2025/08/22 01:26:13 Collector: Worker 1 processed job 80 in 329.306792ms
2025/08/22 01:26:13 Producer created job 84
2025/08/22 01:26:13 Producer created job 85
2025/08/22 01:26:13 Collector: Worker 2 processed job 83 in 224.704917ms
2025/08/22 01:26:13 Producer created job 86
2025/08/22 01:26:13 Collector: Worker 3 processed job 82 in 417.091833ms
2025/08/22 01:26:13 Producer created job 87
2025/08/22 01:26:13 Producer created job 88
2025/08/22 01:26:14 Collector: Worker 1 processed job 84 in 477.999375ms
2025/08/22 01:26:14 Producer created job 89
2025/08/22 01:26:14 Collector: Worker 2 processed job 85 in 377.162042ms
2025/08/22 01:26:14 Producer created job 90
2025/08/22 01:26:14 Collector: Worker 3 processed job 86 in 457.313791ms
2025/08/22 01:26:14 Producer created job 91
2025/08/22 01:26:14 Collector: Worker 2 processed job 88 in 222.092ms
2025/08/22 01:26:14 Collector: Worker 3 processed job 89 in 110.194ms
2025/08/22 01:26:14 Producer created job 92
2025/08/22 01:26:14 Collector: Worker 1 processed job 87 in 361.581ms
2025/08/22 01:26:14 Producer created job 93
2025/08/22 01:26:14 Producer created job 94
2025/08/22 01:26:14 Collector: Worker 3 processed job 91 in 266.266541ms
2025/08/22 01:26:14 Collector: Worker 2 processed job 90 in 348.111416ms
2025/08/22 01:26:14 Producer created job 95
2025/08/22 01:26:14 Collector: Worker 1 processed job 92 in 274.030292ms
2025/08/22 01:26:14 Collector: Worker 3 processed job 93 in 107.0895ms
2025/08/22 01:26:14 Producer created job 96
2025/08/22 01:26:14 Collector: Worker 2 processed job 94 in 181.956583ms
2025/08/22 01:26:14 Producer created job 97
2025/08/22 01:26:14 Producer created job 98
2025/08/22 01:26:15 Producer created job 99
2025/08/22 01:26:15 Collector: Worker 2 processed job 97 in 221.960458ms
2025/08/22 01:26:15 Producer created job 100
2025/08/22 01:26:15 Collector: Worker 1 failed job 95: simulated error on job 95
2025/08/22 01:26:15 Collector: Worker 3 processed job 96 in 413.399959ms
2025/08/22 01:26:15 Producer created job 101
2025/08/22 01:26:15 Producer created job 102
2025/08/22 01:26:15 Collector: Worker 2 failed job 98: simulated error on job 98
2025/08/22 01:26:15 Producer created job 103
2025/08/22 01:26:15 Collector: Worker 2 processed job 101 in 127.873542ms
2025/08/22 01:26:15 Collector: Worker 1 processed job 99 in 342.053416ms
2025/08/22 01:26:15 Collector: Worker 3 failed job 100: simulated error on job 100
2025/08/22 01:26:15 Producer created job 104
2025/08/22 01:26:15 Collector: Worker 2 processed job 102 in 142.174291ms
2025/08/22 01:26:15 Producer created job 105
2025/08/22 01:26:15 Supervisor: removed worker 3 (remaining workers: 2)
2025/08/22 01:26:15 Producer created job 106
2025/08/22 01:26:15 Producer created job 107
2025/08/22 01:26:15 Collector: Worker 3 processed job 104 in 367.7195ms
2025/08/22 01:26:15 Collector: Worker 1 processed job 103 in 450.09575ms
2025/08/22 01:26:15 Producer created job 108
2025/08/22 01:26:16 Collector: Worker 3 processed job 106 in 104.231334ms
2025/08/22 01:26:16 Producer created job 109
2025/08/22 01:26:16 Collector: Worker 2 processed job 105 in 402.292ms
2025/08/22 01:26:16 Producer created job 110
2025/08/22 01:26:16 Collector: Worker 1 processed job 107 in 259.707292ms
2025/08/22 01:26:16 Producer created job 111
2025/08/22 01:26:16 Producer created job 112
2025/08/22 01:26:16 Collector: Worker 2 processed job 108 in 371.266333ms
2025/08/22 01:26:16 Producer created job 113
2025/08/22 01:26:16 Collector: Worker 1 processed job 109 in 261.27925ms
2025/08/22 01:26:16 Producer created job 114
2025/08/22 01:26:16 Producer created job 115
2025/08/22 01:26:16 Supervisor: added worker 3 (total workers: 3)
2025/08/22 01:26:16 Collector: Worker 1 processed job 111 in 224.069292ms
2025/08/22 01:26:16 Producer created job 116
2025/08/22 01:26:16 Producer created job 117
2025/08/22 01:26:16 Collector: Worker 3 processed job 112 in 218.8725ms
2025/08/22 01:26:16 Collector: Worker 2 processed job 110 in 479.12325ms
2025/08/22 01:26:16 Collector: Worker 1 processed job 113 in 257.110708ms
2025/08/22 01:26:16 Producer created job 118
2025/08/22 01:26:17 Producer created job 119
2025/08/22 01:26:17 Producer created job 120
2025/08/22 01:26:17 Supervisor: added worker 4 (total workers: 4)
2025/08/22 01:26:17 Collector: Worker 3 processed job 114 in 325.379375ms
2025/08/22 01:26:17 Producer created job 121
2025/08/22 01:26:17 Collector: Worker 2 processed job 115 in 365.130875ms
2025/08/22 01:26:17 Collector: Worker 1 processed job 116 in 394.753541ms
2025/08/22 01:26:17 Producer created job 122
2025/08/22 01:26:17 Producer created job 123
2025/08/22 01:26:17 Collector: Worker 1 processed job 120 in 120.08825ms
2025/08/22 01:26:17 Producer created job 124
2025/08/22 01:26:17 Collector: Worker 4 processed job 117 in 434.595208ms
2025/08/22 01:26:17 Producer created job 125
2025/08/22 01:26:17 Collector: Worker 2 processed job 119 in 399.066667ms
2025/08/22 01:26:17 Collector: Worker 3 processed job 118 in 470.281416ms
2025/08/22 01:26:17 Producer created job 126
2025/08/22 01:26:17 Collector: Worker 1 processed job 121 in 307.389958ms
2025/08/22 01:26:17 Producer created job 127
2025/08/22 01:26:17 Collector: Worker 2 processed job 123 in 270.876125ms
2025/08/22 01:26:17 Producer created job 128
2025/08/22 01:26:17 Collector: Worker 3 processed job 124 in 293.735584ms
2025/08/22 01:26:18 Collector: Worker 4 processed job 122 in 414.805709ms
2025/08/22 01:26:18 Producer created job 129
2025/08/22 01:26:18 Collector: Worker 2 processed job 126 in 187.870708ms
2025/08/22 01:26:18 Collector: Worker 1 processed job 125 in 354.163916ms
2025/08/22 01:26:18 Producer created job 130
2025/08/22 01:26:18 Supervisor: removed worker 4 (remaining workers: 3)
2025/08/22 01:26:18 Producer created job 131
2025/08/22 01:26:18 Producer created job 132
2025/08/22 01:26:18 Collector: Worker 1 processed job 130 in 212.90375ms
2025/08/22 01:26:18 Collector: Worker 2 processed job 129 in 289.364041ms
2025/08/22 01:26:18 Collector: Worker 3 processed job 127 in 471.080917ms
2025/08/22 01:26:18 Producer created job 133
2025/08/22 01:26:18 Collector: Worker 1 processed job 131 in 106.500667ms
2025/08/22 01:26:18 Collector: Worker 4 processed job 128 in 493.294208ms
2025/08/22 01:26:18 Producer created job 134
2025/08/22 01:26:18 Collector: Worker 2 failed job 132: simulated error on job 132
2025/08/22 01:26:18 Producer created job 135
2025/08/22 01:26:18 Supervisor: removed worker 3 (remaining workers: 2)
2025/08/22 01:26:18 Collector: Worker 3 processed job 133 in 268.983917ms
2025/08/22 01:26:18 Producer created job 136
2025/08/22 01:26:18 Producer created job 137
2025/08/22 01:26:18 Collector: Worker 2 processed job 135 in 239.221ms
2025/08/22 01:26:18 Producer created job 138
2025/08/22 01:26:19 Collector: Worker 2 processed job 136 in 150.158417ms
2025/08/22 01:26:19 Collector: Worker 1 processed job 134 in 493.130959ms
2025/08/22 01:26:19 Producer created job 139
2025/08/22 01:26:19 Producer created job 140
2025/08/22 01:26:19 Collector: Worker 2 processed job 137 in 203.933083ms
2025/08/22 01:26:19 Producer created job 141
2025/08/22 01:26:19 Collector: Worker 1 failed job 138: simulated error on job 138
2025/08/22 01:26:19 Producer created job 142
2025/08/22 01:26:19 Producer created job 143
2025/08/22 01:26:19 Collector: Worker 1 processed job 140 in 234.149625ms
2025/08/22 01:26:19 Producer created job 144
2025/08/22 01:26:19 Collector: Worker 2 processed job 139 in 386.627375ms
2025/08/22 01:26:19 Producer created job 145
2025/08/22 01:26:19 Producer created job 146
2025/08/22 01:26:19 Collector: Worker 1 processed job 141 in 288.161791ms
2025/08/22 01:26:19 Producer created job 147
2025/08/22 01:26:19 Producer created job 148
2025/08/22 01:26:20 Producer created job 149
2025/08/22 01:26:20 Collector: Worker 2 processed job 142 in 490.853542ms
2025/08/22 01:26:20 Producer created job 150
2025/08/22 01:26:20 Supervisor: added worker 3 (total workers: 3)
2025/08/22 01:26:20 Producer created job 151
2025/08/22 01:26:20 Collector: Worker 1 processed job 143 in 493.763584ms
2025/08/22 01:26:20 Producer created job 152
2025/08/22 01:26:20 Collector: Worker 2 processed job 144 in 251.552125ms
2025/08/22 01:26:20 Producer created job 153
2025/08/22 01:26:20 Collector: Worker 1 failed job 146: simulated error on job 146
2025/08/22 01:26:20 Producer created job 154
2025/08/22 01:26:20 Collector: Worker 3 processed job 145 in 419.096417ms
2025/08/22 01:26:20 Producer created job 155
2025/08/22 01:26:20 Supervisor: added worker 4 (total workers: 4)
2025/08/22 01:26:20 Collector: Worker 2 failed job 147: simulated error on job 147
2025/08/22 01:26:20 Producer created job 156
2025/08/22 01:26:20 Producer created job 157
2025/08/22 01:26:20 Producer created job 158
2025/08/22 01:26:20 Collector: Worker 1 processed job 148 in 469.234916ms
2025/08/22 01:26:21 Collector: Worker 4 processed job 150 in 335.069625ms
2025/08/22 01:26:21 Collector: Worker 3 processed job 149 in 422.215209ms
2025/08/22 01:26:21 Producer created job 159
2025/08/22 01:26:21 Collector: Worker 2 processed job 151 in 398.078875ms
2025/08/22 01:26:21 Collector: Worker 3 failed job 154: simulated error on job 154
2025/08/22 01:26:21 Producer created job 160
2025/08/22 01:26:21 Supervisor: added worker 5 (total workers: 5)
2025/08/22 01:26:21 Producer created job 161
2025/08/22 01:26:21 Collector: Worker 2 processed job 155 in 176.734875ms
2025/08/22 01:26:21 Collector: Worker 3 processed job 156 in 158.139ms
2025/08/22 01:26:21 Producer created job 162
2025/08/22 01:26:21 Collector: Worker 1 processed job 152 in 439.556958ms
2025/08/22 01:26:21 Collector: Worker 4 processed job 153 in 426.136167ms
2025/08/22 01:26:21 Producer created job 163
2025/08/22 01:26:21 Producer created job 164
2025/08/22 01:26:21 Collector: Worker 3 processed job 159 in 254.587125ms
2025/08/22 01:26:21 Collector: Worker 5 processed job 157 in 430.081ms
2025/08/22 01:26:21 Collector: Worker 2 processed job 158 in 327.932791ms
2025/08/22 01:26:21 Collector: Worker 1 processed job 160 in 250.284541ms
2025/08/22 01:26:21 Producer created job 165
2025/08/22 01:26:21 Supervisor: removed worker 5 (remaining workers: 4)
2025/08/22 01:26:21 Collector: Worker 4 processed job 161 in 291.145584ms
2025/08/22 01:26:21 Collector: Worker 3 processed job 162 in 172.071583ms
2025/08/22 01:26:21 Collector: Worker 2 processed job 164 in 151.023583ms
2025/08/22 01:26:21 Producer created job 166
2025/08/22 01:26:21 Collector: Worker 5 processed job 163 in 256.11975ms
2025/08/22 01:26:21 Producer created job 167
2025/08/22 01:26:21 Collector: Worker 4 failed job 166: simulated error on job 166
2025/08/22 01:26:21 Producer created job 168
2025/08/22 01:26:22 Producer created job 169
2025/08/22 01:26:22 Collector: Worker 1 processed job 165 in 404.510458ms
2025/08/22 01:26:22 Collector: Worker 2 failed job 168: simulated error on job 168
2025/08/22 01:26:22 Collector: Worker 3 processed job 167 in 290.172125ms
2025/08/22 01:26:22 Producer created job 170
2025/08/22 01:26:22 Supervisor: removed worker 4 (remaining workers: 3)
2025/08/22 01:26:22 Collector: Worker 4 failed job 169: simulated error on job 169
2025/08/22 01:26:22 Producer created job 171
2025/08/22 01:26:22 Producer created job 172
2025/08/22 01:26:22 Collector: Worker 1 processed job 170 in 225.000125ms
2025/08/22 01:26:22 Producer created job 173
2025/08/22 01:26:22 Producer created job 174
2025/08/22 01:26:22 Collector: Worker 2 processed job 171 in 350.595959ms
2025/08/22 01:26:22 Producer created job 175
2025/08/22 01:26:22 Collector: Worker 1 processed job 173 in 202.092708ms
2025/08/22 01:26:22 Supervisor: removed worker 3 (remaining workers: 2)
2025/08/22 01:26:22 Collector: Worker 2 processed job 174 in 123.301792ms
2025/08/22 01:26:22 Producer created job 176
2025/08/22 01:26:22 Collector: Worker 1 processed job 175 in 107.0325ms
2025/08/22 01:26:22 Collector: Worker 3 processed job 172 in 449.279292ms
2025/08/22 01:26:22 Producer created job 177
2025/08/22 01:26:22 Collector: Worker 2 processed job 176 in 132.065333ms
2025/08/22 01:26:22 Producer created job 178
2025/08/22 01:26:23 Producer created job 179
2025/08/22 01:26:23 Collector: Worker 1 processed job 177 in 240.097708ms
2025/08/22 01:26:23 Producer created job 180
2025/08/22 01:26:23 Collector: Worker 2 processed job 178 in 289.144125ms
2025/08/22 01:26:23 Producer created job 181
2025/08/22 01:26:23 Producer created job 182
2025/08/22 01:26:23 Producer created job 183
2025/08/22 01:26:23 Collector: Worker 1 processed job 179 in 399.271958ms
2025/08/22 01:26:23 Producer created job 184
2025/08/22 01:26:23 Producer created job 185
2025/08/22 01:26:23 Collector: Worker 2 processed job 180 in 410.072583ms
2025/08/22 01:26:23 Collector: Worker 1 processed job 181 in 232.16675ms
2025/08/22 01:26:23 Producer created job 186
2025/08/22 01:26:23 Producer created job 187
2025/08/22 01:26:23 Producer created job 188
2025/08/22 01:26:23 Collector: Worker 2 processed job 182 in 321.837417ms
2025/08/22 01:26:24 Producer created job 189
2025/08/22 01:26:24 Collector: Worker 1 processed job 183 in 395.9905ms
2025/08/22 01:26:24 Producer created job 190
2025/08/22 01:26:24 Supervisor: added worker 3 (total workers: 3)
2025/08/22 01:26:24 Producer created job 191
2025/08/22 01:26:24 Collector: Worker 2 processed job 184 in 362.397041ms
2025/08/22 01:26:24 Producer created job 192
2025/08/22 01:26:24 Producer created job 193
2025/08/22 01:26:24 Collector: Worker 1 processed job 185 in 331.17425ms
2025/08/22 01:26:24 Collector: Worker 3 failed job 186: simulated error on job 186
2025/08/22 01:26:24 Producer created job 194
2025/08/22 01:26:24 Collector: Worker 1 failed job 188: simulated error on job 188
2025/08/22 01:26:24 Producer created job 195
2025/08/22 01:26:24 Supervisor: added worker 4 (total workers: 4)
2025/08/22 01:26:24 Collector: Worker 2 processed job 187 in 408.146334ms
2025/08/22 01:26:24 Producer created job 196
2025/08/22 01:26:24 Collector: Worker 3 processed job 189 in 353.135334ms
2025/08/22 01:26:24 Collector: Worker 4 processed job 191 in 173.702583ms
2025/08/22 01:26:24 Producer created job 197
2025/08/22 01:26:24 Collector: Worker 4 processed job 194 in 113.203584ms
2025/08/22 01:26:24 Collector: Worker 1 processed job 190 in 330.401375ms
2025/08/22 01:26:24 Producer created job 198
2025/08/22 01:26:25 Collector: Worker 4 processed job 195 in 105.38125ms
2025/08/22 01:26:25 Producer created job 199
2025/08/22 01:26:25 Collector: Worker 3 processed job 193 in 236.394417ms
2025/08/22 01:26:25 Collector: Worker 1 processed job 196 in 117.889833ms
2025/08/22 01:26:25 Producer created job 200
2025/08/22 01:26:25 Supervisor: error threshold exceeded (13 errors), initiating shutdown
2025/08/22 01:26:25 Producer received shutdown signal, stopping job creation
2025/08/22 01:26:25 Collector (drain): Worker 2 successfully processed job 192 in 463.153208ms
2025/08/22 01:26:25 Collector (drain): Worker 4 successfully processed job 197 in 222.149792ms
2025/08/22 01:26:25 Collector (drain): Worker 3 successfully processed job 198 in 427.713083ms
2025/08/22 01:26:25 Collector (drain): Worker 1 successfully processed job 199 in 484.151542ms
2025/08/22 01:26:25 Application shutdown complete
```

## Bonus: Priority Queues (High vs Normal)

### What this adds
To demonstrate fairness and scheduling under load, the pipeline supports **two input lanes**:
- `highIn` — high‑priority jobs
- `normIn` — normal‑priority jobs

A small goroutine called a **priority multiplexer** ("mux") merges these lanes into the existing worker queue `jobCh`, always **preferring high‑priority** work when it is available. Workers, the supervisor, and the collector remain unchanged.

**Why a mux?** Centralizing priority in a single place keeps the worker hot‑path simple (per‑worker stop remains intact) and preserves the original scaling logic which watches `len(jobCh)`.

### How it works
1. **Producer** emits jobs into `highIn` or `normIn` (default: ~10% high).  
2. **Priority mux** continuously forwards from the two lanes into `jobCh`. It first tries a non‑blocking read from the high lane; if nothing is ready there, it blocks waiting for either lane. This gives high jobs **first dibs** and pushes their queueing latency lower.  
3. **Workers** pull from `jobCh` and process normally.  
4. **Supervisor** still scales based on backlog in `jobCh`.  
5. **Shutdown:** On Ctrl+C (or error‑rate trip), the producer closes both lanes. The mux finishes forwarding any remaining items and then **closes `jobCh`**. Workers finish their current job and exit; the collector drains; the app logs **"Application shutdown complete"**.

### Invariants (safety)
- Only the **producer** closes `highIn` and `normIn`.  
- Only the **mux** closes `jobCh`, and **only after** both input lanes are closed.  
- `resultCh` is closed **after** all workers exit (no send‑to‑closed‑channel).  
- Workers never drop a finished result.

**Example output (priority variant, `go run ./cmd/pipeline-priority`):**

```
2025/08/22 15:02:37 Supervisor: added worker 1 (total workers: 1)
2025/08/22 15:02:37 Producer(N) created job 1
2025/08/22 15:02:37 Collector: [normal] Worker 1 processed job 1 in 115.736708ms
2025/08/22 15:02:37 Producer(N) created job 2
2025/08/22 15:02:37 Producer(N) created job 3
2025/08/22 15:02:37 Producer(N) created job 4
2025/08/22 15:02:37 Collector: [normal] Worker 1 processed job 2 in 367.638542ms
2025/08/22 15:02:37 Producer(H) created job 5
2025/08/22 15:02:38 Producer(N) created job 6
2025/08/22 15:02:38 Producer(N) created job 7
2025/08/22 15:02:38 Collector: [normal] Worker 1 processed job 3 in 475.728333ms
2025/08/22 15:02:38 Producer(N) created job 8
2025/08/22 15:02:38 Producer(N) created job 9
2025/08/22 15:02:38 Producer(N) created job 10
2025/08/22 15:02:38 Collector: [normal] Worker 1 processed job 4 in 474.151375ms
2025/08/22 15:02:38 Producer(H) created job 11
2025/08/22 15:02:38 Collector: [high] Worker 1 processed job 5 in 138.169375ms
2025/08/22 15:02:38 Producer(N) created job 12
2025/08/22 15:02:39 Producer(N) created job 13
2025/08/22 15:02:39 Producer(N) created job 14
2025/08/22 15:02:39 Collector: [normal] Worker 1 processed job 6 in 449.209417ms
2025/08/22 15:02:39 Producer(H) created job 15
2025/08/22 15:02:39 Producer(N) created job 16
2025/08/22 15:02:39 Collector: [normal] Worker 1 failed job 7: simulated error on job 7
2025/08/22 15:02:39 Producer(H) created job 17
2025/08/22 15:02:39 Producer(N) created job 18
2025/08/22 15:02:39 Producer(N) created job 19
2025/08/22 15:02:40 Collector: [normal] Worker 1 processed job 8 in 351.096458ms
2025/08/22 15:02:40 Producer(H) created job 20
2025/08/22 15:02:40 Collector: [normal] Worker 1 processed job 9 in 166.165583ms
2025/08/22 15:02:40 Producer(H) created job 21
2025/08/22 15:02:40 Producer(N) created job 22
2025/08/22 15:02:40 Collector: [normal] Worker 1 processed job 10 in 337.160792ms
2025/08/22 15:02:40 Producer(N) created job 23
2025/08/22 15:02:40 Producer(N) created job 24
2025/08/22 15:02:40 Collector: [high] Worker 1 processed job 11 in 259.112542ms
2025/08/22 15:02:40 Producer(N) created job 25
2025/08/22 15:02:41 Producer(N) created job 26
2025/08/22 15:02:41 Collector: [normal] Worker 1 processed job 12 in 282.325083ms
2025/08/22 15:02:41 Producer(N) created job 27
2025/08/22 15:02:41 Collector: [normal] Worker 1 processed job 13 in 177.167792ms
2025/08/22 15:02:41 Producer(N) created job 28
2025/08/22 15:02:41 Collector: [normal] Worker 1 processed job 14 in 104.070791ms
2025/08/22 15:02:41 Producer(N) created job 29
2025/08/22 15:02:41 Collector: [high] Worker 1 processed job 15 in 249.223333ms
2025/08/22 15:02:41 Producer(N) created job 30
2025/08/22 15:02:41 Producer(N) created job 31
2025/08/22 15:02:41 Producer(N) created job 32
2025/08/22 15:02:42 Collector: [normal] Worker 1 processed job 16 in 440.163375ms
2025/08/22 15:02:42 Producer(N) created job 33
2025/08/22 15:02:42 Producer(N) created job 34
2025/08/22 15:02:42 Producer(N) created job 35
2025/08/22 15:02:42 Collector: [high] Worker 1 processed job 17 in 419.447958ms
2025/08/22 15:02:42 Producer(N) created job 36
2025/08/22 15:02:42 Producer(N) created job 37
2025/08/22 15:02:42 Producer(N) created job 38
2025/08/22 15:02:42 Collector: [normal] Worker 1 processed job 18 in 469.1485ms
2025/08/22 15:02:42 Producer(N) created job 39
2025/08/22 15:02:43 Producer(N) created job 40
2025/08/22 15:02:43 Collector: [normal] Worker 1 processed job 19 in 305.636209ms
2025/08/22 15:02:43 Producer(N) created job 41
2025/08/22 15:02:43 Producer(N) created job 42
2025/08/22 15:02:43 Collector: [high] Worker 1 failed job 20: simulated error on job 20
2025/08/22 15:02:43 Collector: [high] Worker 1 processed job 21 in 138.115667ms
2025/08/22 15:02:43 Producer(N) created job 43
2025/08/22 15:02:43 Producer(N) created job 44
2025/08/22 15:02:43 Collector: [normal] Worker 1 processed job 22 in 293.693709ms
2025/08/22 15:02:43 Producer(N) created job 45
2025/08/22 15:02:44 Producer(N) created job 46
2025/08/22 15:02:44 Producer(N) created job 47
2025/08/22 15:02:44 Collector: [normal] Worker 1 processed job 23 in 443.167417ms
2025/08/22 15:02:44 Producer(N) created job 48
2025/08/22 15:02:44 Producer(N) created job 49
2025/08/22 15:02:44 Producer(N) created job 50
2025/08/22 15:02:44 Supervisor: added worker 2 (total workers: 2)
2025/08/22 15:02:44 Collector: [normal] Worker 1 processed job 24 in 365.75025ms
2025/08/22 15:02:44 Producer(N) created job 51
2025/08/22 15:02:44 Collector: [normal] Worker 2 processed job 25 in 232.60775ms
2025/08/22 15:02:44 Collector: [normal] Worker 1 processed job 26 in 231.084833ms
2025/08/22 15:02:44 Producer(N) created job 52
2025/08/22 15:02:45 Collector: [normal] Worker 2 processed job 27 in 159.135584ms
2025/08/22 15:02:45 Producer(N) created job 53
2025/08/22 15:02:45 Collector: [normal] Worker 2 failed job 29: simulated error on job 29
2025/08/22 15:02:45 Collector: [normal] Worker 1 failed job 28: simulated error on job 28
2025/08/22 15:02:45 Producer(N) created job 54
2025/08/22 15:02:45 Producer(H) created job 55
2025/08/22 15:02:45 Collector: [normal] Worker 2 processed job 30 in 338.1885ms
2025/08/22 15:02:45 Producer(N) created job 56
2025/08/22 15:02:45 Collector: [normal] Worker 1 failed job 31: simulated error on job 31
2025/08/22 15:02:45 Producer(N) created job 57
2025/08/22 15:02:45 Producer(N) created job 58
2025/08/22 15:02:45 Collector: [normal] Worker 2 processed job 32 in 355.090083ms
2025/08/22 15:02:45 Producer(N) created job 59
2025/08/22 15:02:46 Collector: [normal] Worker 1 processed job 33 in 439.613958ms
2025/08/22 15:02:46 Producer(N) created job 60
2025/08/22 15:02:46 Supervisor: added worker 3 (total workers: 3)
2025/08/22 15:02:46 Collector: [normal] Worker 2 processed job 34 in 320.150083ms
2025/08/22 15:02:46 Producer(N) created job 61
2025/08/22 15:02:46 Producer(N) created job 62
2025/08/22 15:02:46 Collector: [normal] Worker 1 processed job 35 in 329.690209ms
2025/08/22 15:02:46 Collector: [normal] Worker 2 processed job 37 in 274.06275ms
2025/08/22 15:02:46 Producer(N) created job 63
2025/08/22 15:02:46 Collector: [normal] Worker 3 processed job 36 in 472.067542ms
2025/08/22 15:02:46 Producer(N) created job 64
2025/08/22 15:02:46 Collector: [normal] Worker 2 processed job 39 in 267.181458ms
2025/08/22 15:02:46 Producer(N) created job 65
2025/08/22 15:02:46 Collector: [normal] Worker 1 processed job 38 in 449.092083ms
2025/08/22 15:02:46 Collector: [normal] Worker 3 processed job 40 in 323.090292ms
2025/08/22 15:02:47 Producer(N) created job 66
2025/08/22 15:02:47 Producer(H) created job 67
2025/08/22 15:02:47 Collector: [normal] Worker 3 processed job 43 in 251.013917ms
2025/08/22 15:02:47 Collector: [normal] Worker 2 failed job 41: simulated error on job 41
2025/08/22 15:02:47 Collector: [normal] Worker 1 processed job 42 in 298.074958ms
2025/08/22 15:02:47 Producer(H) created job 68
2025/08/22 15:02:47 Collector: [normal] Worker 1 processed job 46 in 203.587291ms
2025/08/22 15:02:47 Collector: [normal] Worker 2 processed job 45 in 254.028209ms
2025/08/22 15:02:47 Producer(N) created job 69
2025/08/22 15:02:47 Collector: [normal] Worker 3 processed job 44 in 323.059ms
2025/08/22 15:02:47 Collector: [normal] Worker 1 processed job 47 in 158.806917ms
2025/08/22 15:02:47 Collector: [normal] Worker 2 failed job 48: simulated error on job 48
2025/08/22 15:02:47 Producer(H) created job 70
2025/08/22 15:02:47 Collector: [normal] Worker 1 failed job 50: simulated error on job 50
2025/08/22 15:02:47 Producer(N) created job 71
2025/08/22 15:02:47 Collector: [normal] Worker 2 processed job 51 in 252.093833ms
2025/08/22 15:02:47 Collector: [normal] Worker 3 failed job 49: simulated error on job 49
2025/08/22 15:02:47 Producer(N) created job 72
2025/08/22 15:02:48 Collector: [high] Worker 2 processed job 55 in 235.138334ms
2025/08/22 15:02:48 Producer(N) created job 73
2025/08/22 15:02:48 Collector: [normal] Worker 1 processed job 52 in 489.136958ms
2025/08/22 15:02:48 Collector: [normal] Worker 3 processed job 53 in 271.048625ms
2025/08/22 15:02:48 Producer(N) created job 74
2025/08/22 15:02:48 Collector: [normal] Worker 2 processed job 54 in 283.146333ms
2025/08/22 15:02:48 Collector: [normal] Worker 1 processed job 56 in 209.480125ms
2025/08/22 15:02:48 Producer(N) created job 75
2025/08/22 15:02:48 Collector: [normal] Worker 2 processed job 58 in 102.164958ms
2025/08/22 15:02:48 Producer(N) created job 76
2025/08/22 15:02:48 Collector: [normal] Worker 3 processed job 57 in 373.44825ms
2025/08/22 15:02:48 Collector: [normal] Worker 3 processed job 61 in 107.061625ms
2025/08/22 15:02:48 Producer(N) created job 77
2025/08/22 15:02:48 Producer(N) created job 78
2025/08/22 15:02:48 Collector: [normal] Worker 1 processed job 59 in 497.057709ms
2025/08/22 15:02:48 Collector: [normal] Worker 2 failed job 60: simulated error on job 60
2025/08/22 15:02:48 Collector: [normal] Worker 3 processed job 62 in 277.1335ms
2025/08/22 15:02:48 Producer(N) created job 79
2025/08/22 15:02:49 Collector: [normal] Worker 1 processed job 63 in 178.171917ms
2025/08/22 15:02:49 Collector: [high] Worker 3 processed job 67 in 167.422375ms
2025/08/22 15:02:49 Producer(H) created job 80
2025/08/22 15:02:49 Producer(N) created job 81
2025/08/22 15:02:49 Collector: [normal] Worker 2 processed job 64 in 460.119125ms
2025/08/22 15:02:49 Collector: [normal] Worker 1 processed job 65 in 363.1715ms
2025/08/22 15:02:49 Producer(N) created job 82
2025/08/22 15:02:49 Collector: [normal] Worker 3 processed job 66 in 445.484291ms
2025/08/22 15:02:49 Producer(N) created job 83
2025/08/22 15:02:49 Collector: [high] Worker 2 processed job 68 in 253.141666ms
2025/08/22 15:02:49 Collector: [normal] Worker 1 processed job 69 in 295.069792ms
2025/08/22 15:02:49 Producer(N) created job 84
2025/08/22 15:02:49 Producer(N) created job 85
2025/08/22 15:02:49 Collector: [normal] Worker 1 processed job 72 in 224.066209ms
2025/08/22 15:02:49 Collector: [normal] Worker 2 processed job 71 in 351.314167ms
2025/08/22 15:02:50 Collector: [high] Worker 3 processed job 70 in 471.273875ms
2025/08/22 15:02:50 Producer(N) created job 86
2025/08/22 15:02:50 Collector: [normal] Worker 1 processed job 73 in 170.062583ms
2025/08/22 15:02:50 Producer(N) created job 87
2025/08/22 15:02:50 Collector: [normal] Worker 2 processed job 74 in 374.161083ms
2025/08/22 15:02:50 Producer(N) created job 88
2025/08/22 15:02:50 Producer(N) created job 89
2025/08/22 15:02:50 Collector: [normal] Worker 3 processed job 75 in 472.5125ms
2025/08/22 15:02:50 Collector: [normal] Worker 1 processed job 76 in 402.166833ms
2025/08/22 15:02:50 Collector: [normal] Worker 2 processed job 77 in 213.875167ms
2025/08/22 15:02:50 Producer(H) created job 90
2025/08/22 15:02:50 Collector: [normal] Worker 1 processed job 79 in 194.164333ms
2025/08/22 15:02:50 Collector: [normal] Worker 3 processed job 78 in 261.257083ms
2025/08/22 15:02:50 Producer(N) created job 91
2025/08/22 15:02:50 Collector: [high] Worker 2 processed job 80 in 284.102792ms
2025/08/22 15:02:50 Producer(N) created job 92
2025/08/22 15:02:51 Collector: [normal] Worker 1 processed job 81 in 325.167625ms
2025/08/22 15:02:51 Collector: [normal] Worker 2 failed job 83: simulated error on job 83
2025/08/22 15:02:51 Producer(N) created job 93
2025/08/22 15:02:51 Collector: [normal] Worker 3 processed job 82 in 366.107458ms
2025/08/22 15:02:51 Collector: [normal] Worker 1 processed job 84 in 160.079584ms
2025/08/22 15:02:51 Producer(N) created job 94
2025/08/22 15:02:51 Collector: [normal] Worker 3 processed job 86 in 217.17925ms
2025/08/22 15:02:51 Collector: [normal] Worker 2 failed job 85: simulated error on job 85
2025/08/22 15:02:51 Producer(N) created job 95
2025/08/22 15:02:51 Collector: [normal] Worker 3 failed job 88: simulated error on job 88
2025/08/22 15:02:51 Collector: [normal] Worker 1 failed job 87: simulated error on job 87
2025/08/22 15:02:51 Producer(N) created job 96
2025/08/22 15:02:51 Collector: [normal] Worker 2 processed job 89 in 299.108958ms
2025/08/22 15:02:51 Producer(N) created job 97
2025/08/22 15:02:51 Collector: [normal] Worker 2 failed job 92: simulated error on job 92
2025/08/22 15:02:51 Collector: [normal] Worker 1 processed job 91 in 256.096667ms
2025/08/22 15:02:51 Collector: [high] Worker 3 processed job 90 in 271.040958ms
2025/08/22 15:02:51 Producer(N) created job 98
2025/08/22 15:02:51 Collector: [normal] Worker 1 processed job 94 in 113.161834ms
2025/08/22 15:02:51 Producer(N) created job 99
2025/08/22 15:02:52 Producer(N) created job 100
2025/08/22 15:02:52 Collector: [normal] Worker 3 failed job 95: simulated error on job 95
2025/08/22 15:02:52 Collector: [normal] Worker 2 processed job 93 in 470.118334ms
2025/08/22 15:02:52 Producer(N) created job 101
2025/08/22 15:02:52 Collector: [normal] Worker 3 processed job 97 in 108.561666ms
2025/08/22 15:02:52 Collector: [normal] Worker 1 failed job 96: simulated error on job 96
2025/08/22 15:02:52 Producer(N) created job 102
2025/08/22 15:02:52 Collector: [normal] Worker 2 processed job 98 in 261.188375ms
2025/08/22 15:02:52 Collector: [normal] Worker 1 processed job 100 in 158.611792ms
2025/08/22 15:02:52 Producer(H) created job 103
2025/08/22 15:02:52 Collector: [normal] Worker 3 processed job 99 in 369.125625ms
2025/08/22 15:02:52 Producer(N) created job 104
2025/08/22 15:02:52 Producer(N) created job 105
2025/08/22 15:02:52 Collector: [normal] Worker 2 processed job 101 in 431.113ms
2025/08/22 15:02:52 Collector: [normal] Worker 1 processed job 102 in 480.132875ms
2025/08/22 15:02:53 Collector: [high] Worker 3 processed job 103 in 364.04875ms
2025/08/22 15:02:53 Producer(N) created job 106
2025/08/22 15:02:53 Supervisor: removed worker 3 (remaining workers: 2)
2025/08/22 15:02:53 Producer(N) created job 107
2025/08/22 15:02:53 Collector: [normal] Worker 2 processed job 104 in 260.149833ms
2025/08/22 15:02:53 Producer(H) created job 108
2025/08/22 15:02:53 Collector: [normal] Worker 1 processed job 105 in 343.291292ms
2025/08/22 15:02:53 Collector: [normal] Worker 3 failed job 106: simulated error on job 106
2025/08/22 15:02:53 Collector: [normal] Worker 2 processed job 107 in 213.047708ms
2025/08/22 15:02:53 Producer(N) created job 109
2025/08/22 15:02:53 Producer(N) created job 110
2025/08/22 15:02:53 Producer(N) created job 111
2025/08/22 15:02:53 Collector: [high] Worker 1 processed job 108 in 481.070917ms
2025/08/22 15:02:53 Collector: [normal] Worker 2 failed job 109: simulated error on job 109
2025/08/22 15:02:53 Producer(N) created job 112
2025/08/22 15:02:54 Producer(N) created job 113
2025/08/22 15:02:54 Collector: [normal] Worker 1 processed job 110 in 308.242583ms
2025/08/22 15:02:54 Collector: [normal] Worker 2 processed job 111 in 341.066541ms
2025/08/22 15:02:54 Producer(N) created job 114
2025/08/22 15:02:54 Collector: [normal] Worker 2 processed job 113 in 188.179833ms
2025/08/22 15:02:54 Producer(N) created job 115
2025/08/22 15:02:54 Collector: [normal] Worker 2 failed job 114: simulated error on job 114
2025/08/22 15:02:54 Producer(N) created job 116
2025/08/22 15:02:54 Collector: [normal] Worker 1 processed job 112 in 461.161417ms
2025/08/22 15:02:54 Supervisor: removed worker 2 (remaining workers: 1)
2025/08/22 15:02:54 Producer(N) created job 117
2025/08/22 15:02:54 Collector: [normal] Worker 2 processed job 115 in 205.117458ms
2025/08/22 15:02:54 Producer(N) created job 118
2025/08/22 15:02:54 Producer(N) created job 119
2025/08/22 15:02:55 Collector: [normal] Worker 1 processed job 116 in 468.156125ms
2025/08/22 15:02:55 Producer(N) created job 120
2025/08/22 15:02:55 Producer(N) created job 121
2025/08/22 15:02:55 Collector: [normal] Worker 1 processed job 117 in 292.131208ms
2025/08/22 15:02:55 Producer(N) created job 122
2025/08/22 15:02:55 Producer(N) created job 123
2025/08/22 15:02:55 Producer(N) created job 124
2025/08/22 15:02:55 Collector: [normal] Worker 1 processed job 118 in 494.148541ms
2025/08/22 15:02:55 Producer(N) created job 125
2025/08/22 15:02:56 Producer(N) created job 126
2025/08/22 15:02:56 Collector: [normal] Worker 1 failed job 119: simulated error on job 119
2025/08/22 15:02:56 Producer(N) created job 127
2025/08/22 15:02:56 Producer(H) created job 128
2025/08/22 15:02:56 Collector: [normal] Worker 1 processed job 120 in 352.173125ms
2025/08/22 15:02:56 Producer(N) created job 129
2025/08/22 15:02:56 Producer(N) created job 130
2025/08/22 15:02:56 Collector: [normal] Worker 1 failed job 121: simulated error on job 121
2025/08/22 15:02:56 Producer(N) created job 131
2025/08/22 15:02:56 Collector: [normal] Worker 1 failed job 122: simulated error on job 122
2025/08/22 15:02:56 Producer(N) created job 132
2025/08/22 15:02:57 Collector: [normal] Worker 1 processed job 123 in 155.249375ms
2025/08/22 15:02:57 Producer(N) created job 133
2025/08/22 15:02:57 Supervisor: error threshold exceeded (18 errors), initiating shutdown
2025/08/22 15:02:57 Producer received shutdown signal, stopping job creation
2025/08/22 15:02:57 Collector (drain): Worker 1 successfully processed job 124 in 321.122291ms
2025/08/22 15:02:57 Collector (drain): Worker 1 successfully processed job 125 in 129.129084ms
2025/08/22 15:02:57 Application shutdown complete
```
