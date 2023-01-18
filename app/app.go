package app

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
	"gitlab.com/gigaming/igaming/serverless/go-match-queue/jobqueue"
)

type App struct {
	Timeout time.Duration
}

func NewApp(timeout time.Duration) *App {
	return &App{
		Timeout: timeout,
	}
}

func (a *App) StartApplication() {
	ctx := context.TODO()

	// Create a queue and assign a processor to the queue
	firstQueue := jobqueue.NewQueue("odd-queue", GetOddNumbersFromRandom, jobqueue.QueueOpts{
		Concurrency: 1,
		Timeout:     time.Second,
		Ticker:      time.Second,
	})

	// Create a queue and assign a processor to the queue
	secondQueue := jobqueue.NewQueue("even-queue", GetEvenNumbersFromRandom, jobqueue.QueueOpts{
		Concurrency: 1,
		Timeout:     time.Second,
		Ticker:      time.Second * 5,
	})

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	qclient, err := jobqueue.NewQueueClient(ctx, redisClient)
	if err != nil {
		panic(err)
	}

	qclient.AddQueue(firstQueue)
	qclient.AddQueue(secondQueue)
	qclient.StartOperation(ctx)
}

func GetOddNumbersFromRandom(ctx context.Context, data interface{}) error {
	randomInt := rand.Intn(100)
	time.Sleep(time.Duration(rand.Intn(1500)) * time.Millisecond)
	if randomInt%2 == 0 {
		return errors.New(fmt.Sprintf("Failed since the result is even. Value: %v", randomInt))
	}
	return nil
}

func GetEvenNumbersFromRandom(ctx context.Context, data interface{}) error {
	randomInt := rand.Intn(100)
	time.Sleep(time.Duration(rand.Intn(1500)) * time.Millisecond)
	if randomInt%2 != 0 {
		return errors.New(fmt.Sprintf("Failed since the result is odd. Value: %v", randomInt))
	}
	return nil
}
