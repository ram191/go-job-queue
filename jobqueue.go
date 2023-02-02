package gojobqueue

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	fmt.Println("vim-go")
}

type QueueClient struct {
	RedisClient *redis.Client
	Queues      []Queue
}

// A job assigned to a queue
type Job struct {
	Id            string
	Completed     bool
	Data          interface{}
	Output        interface{}
	ExecutionTime time.Duration
	Error         chan error
}
type Worker struct {
	StopStatus chan struct{}
}

// Create a new queue client
func NewQueueClient(ctx context.Context, redisClient *redis.Client) (client *QueueClient, err error) {
	client = &QueueClient{
		RedisClient: redisClient,
	}
	return client, err
}

// Add a queue to queue client
func (qc *QueueClient) AddQueue(queue Queue) {
	queue.RedisClient = qc.RedisClient
	qc.Queues = append(qc.Queues, queue)
}

func (qc *QueueClient) StartOperation(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	for i := 0; i < len(qc.Queues); i++ {
		wg.Add(1)
		queue := qc.Queues[i]
		go func() {
			// Go routine listens to context channel on cancellation
			go func() {
				select {
				case <-ctx.Done():
					fmt.Printf("Stopping queue %v\n", queue.Name)
					wg.Done()
				}
			}()

			queue.Start(ctx)
		}()
	}

	// Go routine waiting for stop signal
	go func() {
		for {
			select {
			case <-sigs:
				fmt.Print("TERMINATING....\n")
				cancel()
			default:
			}
		}
	}()
	time.Sleep(time.Second)
	wg.Wait()
}

// Register the queue
func (qc *QueueClient) registerQueues(ctx context.Context) error {
	for _, q := range qc.Queues {
		err := q.RegisterQueue(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
