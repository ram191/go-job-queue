package gojobqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// A queue contains a name identifier, a channel to hold the jobs, and Run method to run the processor assigned to the queue
type Queue struct {
	Name        string
	Channel     chan Job
	Run         func(context.Context, interface{}) (interface{}, error)
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
func NewQueue(name string, processor func(ctx context.Context, data interface{}) (interface{}, error), opts QueueOpts) Queue {
	fmt.Printf("Queue %v has been created!\n", name)
	return Queue{
		Name:        name,
		Channel:     make(chan Job, 100000),
		Run:         processor,
		Concurrency: opts.Concurrency,
		Timeout:     opts.Timeout,
		Ticker:      opts.Ticker,
	}
}

// Push a job to a queue
func (q Queue) PushJob(ctx context.Context, job Job) error {
	fmt.Printf("Added job with id %v to %v queue\n", job.Id, q.Name)

	// Add job to Redis
	err := q.AddJobToRedis(ctx, job)
	if err != nil {
		fmt.Printf("Failed job with id %v to %v queue\n: %v", job.Id, q.Name, err.Error())
		return err
	}

	// Add to jobs channel
	q.Channel <- job

	return nil
}

// Consume jobs from the queue
func (q Queue) Start(ctx context.Context) {
	var wg sync.WaitGroup
	fmt.Printf("Start processing %v queue....\n", q.Name)

	// Fetch jobs from Redis
	if err := q.FetchJobsFromRedis(ctx); err != nil {
		fmt.Printf("Error fetching jobs from Redis: %v\n", err.Error())
	}

	// Create repeat job go routine
	go func() {
		wg.Add(1)
		ticker := time.NewTicker(q.Ticker)

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
				now := time.Now().Unix()
				job := Job{
					Id:   fmt.Sprintf("repeat:%v", now),
					Data: q.Data,
				}
				time.AfterFunc(q.Ticker, func() { q.PushJob(ctx, job) })
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
					out, err := q.Run(jctx, j.Data)
					j.Error <- err
					j.Output = out

					select {
					case err := <-j.Error:
						if err != nil {
							fmt.Printf("Job %v failed: %v\n", j.Id, err.Error())
						} else {
							fmt.Printf("Job %v success\n", j.Id)
						}
						if err := q.CompleteJobRedis(ctx, j, err); err != nil {
							fmt.Printf("Failed updating complete job to Redis: %v\n", err.Error())
						}
						cancel()
					case <-jctx.Done():
						fmt.Printf("Job %v timed out\n", j.Id)
						if err := q.CompleteJobRedis(ctx, j, errors.New("job timed out")); err != nil {
							fmt.Printf("Failed updating complete job to Redis: %v\n", err.Error())
						}
						cancel()
					}
				}
			}
		}(ctx)
	}

	time.Sleep(time.Second)
	wg.Wait()
}
