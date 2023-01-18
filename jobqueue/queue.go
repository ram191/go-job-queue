package jobqueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// A queue contains a name identifier, a channel to hold the jobs, and Run method to run the processor assigned to the queue
type Queue struct {
	Name        string
	Channel     chan Job
	Run         func(context.Context, interface{}) error
	Data        interface{}
	Concurrency int
	Timeout     time.Duration
	Workers     []Worker
	Ticker      time.Duration
	RedisClient *redis.Client
}

type QueueOpts struct {
	Concurrency int
	Timeout     time.Duration
	Ticker      time.Duration
}

// Create a new queue
func NewQueue(name string, processor func(ctx context.Context, data interface{}) error, opts QueueOpts) Queue {
	fmt.Printf("Queue %v has been created!\n", name)
	return Queue{
		Name:        name,
		Channel:     make(chan Job, 100),
		Run:         processor,
		Concurrency: opts.Concurrency,
		Timeout:     opts.Timeout,
		Ticker:      opts.Ticker,
	}
}

// Push a job to a queue
func (q Queue) PushJob(job Job) {
	fmt.Printf("Added job with id %v to %v queue\n", job.Id, q.Name)

	// Add job to Redis
	q.Channel <- job
}

// Consume jobs from the queue
func (q Queue) Start(ctx context.Context) {
	var wg sync.WaitGroup
	fmt.Printf("Start processing %v queue....\n", q.Name)

	// Create repeat job go routine
	go func() {
		wg.Add(1)
		ticker := time.NewTicker(q.Ticker)
		job := Job{
			Id:   fmt.Sprintf("repeat:%v", time.Now().Unix()),
			Data: q.Data,
		}

		// Go routine listens to context channel on cancellation
		go func() {
			select {
			case <-ctx.Done():
				fmt.Print("Stopping repeat job\n")
				wg.Done()
			}
		}()

		// Push a new job every ticker time
		for {
			select {
			case <-ticker.C:
				time.AfterFunc(q.Ticker, func() { q.PushJob(job) })
			}
		}
	}()

	// Go routines assigned to run the jobs
	for i := 0; i < q.Concurrency; i++ {
		go func(ctx context.Context) {
			wg.Add(1)
			for {
				select {
				case <-ctx.Done():
					fmt.Print("Stopping concurrent job\n")
					wg.Done()
					return
				case j := <-q.Channel:
					jctx, cancel := context.WithTimeout(context.Background(), q.Timeout)
					j.Error = make(chan error, 1)
					j.Error <- q.Run(jctx, j.Data)

					select {
					case err := <-j.Error:
						if err != nil {
							fmt.Printf("Job %v failed: %v\n", j.Id, err.Error())
							cancel()
						} else {
							fmt.Printf("Job %v success\n", j.Id)
							cancel()
						}
					case <-jctx.Done():
						fmt.Printf("Job %v timed out\n", j.Id)
						cancel()
					}
				}
			}
		}(ctx)
	}

	time.Sleep(time.Second)
	wg.Wait()
}
