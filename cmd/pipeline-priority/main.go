package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

// Job represents a unit of work in the pipeline. Each job has a unique ID.
type Job struct {
	ID       int
	Priority string // "high" or "normal"
}

// Result captures the outcome of processing a Job. If Error is non-nil,
// processing failed.
type Result struct {
	JobID    int
	WorkerID int
	Priority string
	Error    error
	Duration time.Duration
}

// worker encapsulates a single worker goroutine. Each worker has its own
// stop channel allowing the supervisor to gracefully shut down individual
// workers without interrupting others. Once stopCh is closed the worker
// finishes its current job and exits.
type worker struct {
	id     int
	stopCh chan struct{}
}

// startWorker launches a goroutine that consumes jobs from jobCh, processes
// them, and publishes results on resultCh. The worker exits when either the
// shared context is cancelled (graceful shutdown for the entire system), its
// individual stop channel is closed (removal of this specific worker), or the
// job channel is closed.
func startWorker(ctx context.Context, w worker, jobCh <-chan Job, resultCh chan<- Result, wg *sync.WaitGroup, errorProb float64) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		// shouldStop becomes true after the stop channel is closed. Once
		// shouldStop is true the worker will exit after finishing the current job.
		shouldStop := false
		for {
			// If we've been asked to stop and there is no job to process, exit.
			if shouldStop {
				return
			}
			// Non-blocking check for stop signal. We can't rely solely on a
			// blocking read because if the job channel is always ready we might
			// never select the stop case. Using a non-blocking check allows
			// reacting promptly to stop requests.
			select {
			case <-ctx.Done():
				// Global shutdown: stop immediately (after finishing current job).
				return
			case <-w.stopCh:
				// Mark that we've received a stop signal. The next iteration
				// after processing the current job will exit.
				shouldStop = true
			default:
				// fall through to job processing
			}
			// Receive a job. If the job channel is closed, exit.
			select {
			case <-ctx.Done():
				return
			case job, ok := <-jobCh:
				if !ok {
					// The producer closed the channel. No more jobs to process.
					return
				}
				start := time.Now()
				// Simulate processing time (between 100-500ms) to emulate real work.
				processTime := time.Duration(100+rand.Intn(400)) * time.Millisecond
				time.Sleep(processTime)
				var err error
				// Randomly inject failures based on error probability.
				if rand.Float64() < errorProb {
					err = fmt.Errorf("simulated error on job %d", job.ID)
				}
				duration := time.Since(start)
				result := Result{
					JobID:    job.ID,
					WorkerID: w.id,
					Priority: job.Priority,
					Error:    err,
					Duration: duration,
				}
				// Publish the result.

				resultCh <- result

				// After finishing the job check if we should exit.
				if shouldStop {
					return
				}
			case <-w.stopCh:
				// Received a stop request before a job arrives. Set flag and
				// continue to look for a job. We don't exit until after
				// processing the current job (if any) to ensure graceful
				// shutdown of this worker.
				shouldStop = true
			}
		}
	}()
}

// producer generates jobs and sends them into jobCh until the context is
// cancelled. When cancelled, the producer closes jobCh to signal there will
// be no more jobs. The pacing of job creation is intentionally slower than
// potential worker throughput to allow the queue to fill and exercise the
// dynamic scaling logic.
// producer now writes to highCh or normCh based on highPct.
func producer(ctx context.Context, highCh, normCh chan<- Job, initialID int, wg *sync.WaitGroup, highPct float64, interval time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		jobID := initialID
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("Producer received shutdown signal, stopping job creation")
				// Close both lanes: mux will close the merged out channel.
				close(highCh)
				close(normCh)
				return
			case <-ticker.C:
				prio := "normal"
				if rand.Float64() < highPct {
					prio = "high"
				}
				job := Job{ID: jobID, Priority: prio}
				jobID++

				if prio == "high" {
					select {
					case highCh <- job:
						log.Printf("Producer(H) created job %d", job.ID)
					case <-ctx.Done():
						log.Println("Producer interrupted during send (high), closing")
						close(highCh)
						close(normCh)
						return
					}
				} else {
					select {
					case normCh <- job:
						log.Printf("Producer(N) created job %d", job.ID)
					case <-ctx.Done():
						log.Println("Producer interrupted during send (normal), closing")
						close(highCh)
						close(normCh)
						return
					}
				}
			}
		}
	}()
}

// supervisor monitors the job queue length and adjusts the number of workers
// accordingly. It also monitors the error rate by reading from the errorCount
// atomic counter. If too many errors occur within a sampling interval it
// triggers a global shutdown by cancelling the context. The supervisor stops
// when the context is cancelled.
func supervisor(ctx context.Context, highIn, normIn, jobCh chan Job, resultCh chan Result, workerWG *sync.WaitGroup, globalCancel context.CancelFunc, minWorkers, maxWorkers int, scaleUpThresh, scaleDownThresh int, errorThreshold int, errorWindow time.Duration, errorCounter *int64) {
	// Maintain a slice of active workers and their stop channels.
	var workers []worker
	nextWorkerID := 1
	// Helper to add a worker.
	addWorker := func() {
		w := worker{id: nextWorkerID, stopCh: make(chan struct{})}
		nextWorkerID++
		startWorker(ctx, w, jobCh, resultCh, workerWG, 0.2)
		workers = append(workers, w)
		log.Printf("Supervisor: added worker %d (total workers: %d)", w.id, len(workers))
	}
	// Helper to remove the last worker.
	removeWorker := func() {
		if len(workers) == 0 {
			return
		}
		// Choose the last worker to remove to avoid disturbing earlier workers.
		w := workers[len(workers)-1]
		workers = workers[:len(workers)-1]
		// Closing the stopCh tells the worker to finish its current job then exit.
		close(w.stopCh)
		log.Printf("Supervisor: removed worker %d (remaining workers: %d)", w.id, len(workers))
	}
	// Start with minWorkers workers.
	for i := 0; i < minWorkers; i++ {
		addWorker()
	}
	// Use a ticker to periodically evaluate scaling conditions.
	scaleTicker := time.NewTicker(500 * time.Millisecond)
	defer scaleTicker.Stop()
	// Use another ticker to periodically check error rate.
	errTicker := time.NewTicker(errorWindow)
	defer errTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Global shutdown: stop all workers by closing their stop channels.
			for _, w := range workers {
				close(w.stopCh)
			}
			return
		case <-scaleTicker.C:
			queueLen := len(jobCh) // merged (post-mux)
			highLen := len(highIn)
			normLen := len(normIn)

			numWorkers := len(workers)
			weighted := highLen*2 + normLen // simple bias for high-priority backlog

			if (highLen > 0 && numWorkers < maxWorkers) || (weighted > scaleUpThresh && numWorkers < maxWorkers) {
				addWorker()
			} else if queueLen < scaleDownThresh && highLen == 0 && numWorkers > minWorkers {
				removeWorker()
			}
		case <-errTicker.C:
			// Check error count in this window.
			currentErrors := atomic.LoadInt64(errorCounter)
			if int(currentErrors) >= errorThreshold {
				log.Printf("Supervisor: error threshold exceeded (%d errors), initiating shutdown", currentErrors)
				globalCancel()
			}
			// Reset the counter for the next window.
			atomic.StoreInt64(errorCounter, 0)
		}
	}
}

// priorityMux forwards high-priority jobs ahead of normal ones into out.
// It closes out when both inputs are closed or the context is cancelled.
func priorityMux(ctx context.Context, highCh, normCh <-chan Job, out chan<- Job, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(out)

		highOpen, normOpen := true, true
		for highOpen || normOpen {
			// Try HIGH lane first (non-blocking preference).
			select {
			case j, ok := <-highCh:
				if ok {
					// SEND with cancel escape hatch to avoid deadlock when jobCh is full.
					select {
					case out <- j:
					case <-ctx.Done():
						return
					}
					continue
				}
				highOpen = false
			default:
			}

			// Otherwise wait for either lane, still honoring cancellation.
			select {
			case j, ok := <-highCh:
				if ok {
					select {
					case out <- j:
					case <-ctx.Done():
						return
					}
				} else {
					highOpen = false
				}
			case j, ok := <-normCh:
				if ok {
					select {
					case out <- j:
					case <-ctx.Done():
						return
					}
				} else {
					normOpen = false
				}
			case <-ctx.Done():
				// On global cancel, stop forwarding immediately.
				return
			}
		}
	}()
}

// collector consumes results from resultCh and logs successes and failures. It
// increments the error counter whenever an error is observed. The collector
// exits when both the context is cancelled and the channel is closed and
// drained.
func collector(ctx context.Context, resultCh <-chan Result, wg *sync.WaitGroup, errorCounter *int64) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				// When the context is done, continue draining results until the
				// channel is closed so we don't drop any pending results.
				for res := range resultCh {
					if res.Error != nil {
						log.Printf("Collector (drain): Worker %d failed job %d: %v", res.WorkerID, res.JobID, res.Error)
					} else {
						log.Printf("Collector (drain): Worker %d successfully processed job %d in %v", res.WorkerID, res.JobID, res.Duration)
					}
				}
				return
			case res, ok := <-resultCh:
				if !ok {
					// Channel closed: no more results.
					return
				}
				if res.Error != nil {
					atomic.AddInt64(errorCounter, 1)
					log.Printf("Collector: [%s] Worker %d failed job %d: %v", res.Priority, res.WorkerID, res.JobID, res.Error)
				} else {
					log.Printf("Collector: [%s] Worker %d processed job %d in %v", res.Priority, res.WorkerID, res.JobID, res.Duration)
				}
			}
		}
	}()
}

// run initializes all components (producer, supervisor, workers, collector) and
// waits for a graceful shutdown triggered by either an OS interrupt (Ctrl+C)
// or an excessive error rate. It returns when all goroutines have finished.
func run() {
	rand.Seed(time.Now().UnixNano())
	// Create channels for jobs and results. Use buffered channels to hold a
	// backlog of jobs, which allows us to observe queue growth.
	highIn := make(chan Job, 20)
	normIn := make(chan Job, 20)
	jobCh := make(chan Job, 20)
	resultCh := make(chan Result, 20)
	// Create a root context that we can cancel to initiate shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	// WaitGroups to ensure we wait for all goroutines to finish.
	var producerWG, muxWG, workerWG, collectorWG sync.WaitGroup
	// Counter to track errors for the auto-shutdown feature.
	var errorCounter int64
	// Start the priority mux: forwards high first into jobCh.
	priorityMux(ctx, highIn, normIn, jobCh, &muxWG)
	// Start producer: 10% high-priority jobs every 200ms
	producer(ctx, highIn, normIn, 1, &producerWG, 0.10, 150*time.Millisecond)
	// Start collector.
	collector(ctx, resultCh, &collectorWG, &errorCounter)
	// Start supervisor with parameters:
	// minWorkers = 1, maxWorkers = 5, scaleUpThresh = 3, scaleDownThresh = 1,
	// errorThreshold = 10 errors per 10 second window.
	go supervisor(ctx, highIn, normIn, jobCh, resultCh, &workerWG, cancel, 1, 5, 3, 1, 10, 10*time.Second, &errorCounter)
	// Listen for OS interrupt (Ctrl+C) to initiate shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	select {
	case <-sigCh:
		log.Println("Received interrupt signal, initiating graceful shutdown")
		cancel()
	case <-ctx.Done():
		// Context cancelled by supervisor due to error rate.
	}
	// Wait for all components to finish.
	producerWG.Wait()
	// Close the result channel once all workers have finished sending results.
	// We wait on the worker WaitGroup separately so that workers finish sending.
	muxWG.Wait()
	workerWG.Wait()
	close(resultCh)
	collectorWG.Wait()
	log.Println("Application shutdown complete")
}

// main is the entry point of the program.
func main() {
	run()
}
