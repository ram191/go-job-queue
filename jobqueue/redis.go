package jobqueue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Key examples:
// Queue -> goq:mysamplequeue1
// Job -> goq:mysamplequeue1:myjob1

func (q *Queue) redisKeyQueue() string {
	return fmt.Sprintf("goq:%v", q.Name)
}

func (q *Queue) redisKeyJob(jobId string) string {
	return fmt.Sprintf("goq:%v:%v", q.Name, jobId)
}

// Register queue to Redis
func (q *Queue) RegisterQueue(ctx context.Context) error {
	key := q.redisKeyQueue()
	res := q.RedisClient.HSet(ctx, key, map[string]interface{}{
		"name":        q.Name,
		"concurrency": q.Concurrency,
		"timeout":     q.Timeout,
		"ticker":      q.Ticker,
		"status":      1,
	})

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// Set queue status to active(1) or inactive(0). This is used later to block another queue from running when it is already in a running state
func (q *Queue) SetRedisQueueStatus(ctx context.Context, status int8) error {
	key := q.redisKeyQueue()
	res := q.RedisClient.HSet(ctx, key, map[string]interface{}{
		"status": status,
	})

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// Add a job to Redis
func (q *Queue) AddJobToRedis(ctx context.Context, job Job) error {
	key := q.redisKeyJob(job.Id)
	res := q.RedisClient.HSet(ctx, key, map[string]interface{}{
		"id":        job.Id,
		"completed": false,
		"data":      job.Data,
		"output":    "",
		"execTime":  job.ExecutionTime,
	})

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// Add job completion property values to Redis. This will also expire the key from Redis
func (q *Queue) CompleteJobRedis(ctx context.Context, job Job, err error) error {
	key := q.redisKeyJob(job.Id)
	var errorMessage string
	if err != nil {
		errorMessage = err.Error()
	}

	res := q.RedisClient.HSet(ctx, key, map[string]interface{}{
		"completed": true,
		"output":    job.Output,
		"execTime":  job.ExecutionTime,
		"error":     errorMessage,
	})

	q.RedisClient.Expire(ctx, key, time.Second*10)

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// Fetch jobs from Redis and add them to the Job's channel
func (q *Queue) FetchJobsFromRedis(ctx context.Context) error {
	pattern := q.redisKeyJob("*")
	res := q.RedisClient.Keys(ctx, pattern)
	if res.Err() != nil {
		return res.Err()
	}

	var wg sync.WaitGroup

	for _, k := range res.Val() {
		go func(key string) {
			wg.Add(1)
			var job Job
			redisJob := q.RedisClient.HGetAll(ctx, key)
			job.Id = redisJob.Val()["id"]
			job.Data = redisJob.Val()["data"]
			if redisJob.Val()["completed"] == "true" {
				wg.Done()
				return
			}
			q.Channel <- job
			wg.Done()
		}(k)
	}

	time.Sleep(time.Second)
	wg.Wait()

	fmt.Printf("Fetched %v jobs on queue %v from redis\n", len(q.Channel), q.Name)
	return nil
}
