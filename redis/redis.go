package redis

import (
	"context"

	"github.com/go-redis/redis/v9"
)

func CreateNewClient(ctx context.Context) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: "redis://localhost:6379",
		DB:   0,
	})

	// Ping once to ensure Redis connection
	pingRes := client.Ping(ctx)
	if pingRes.Err() != nil {
		return nil, pingRes.Err()
	}

	return client, nil
}
