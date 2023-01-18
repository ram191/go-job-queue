package main

import (
	"time"

	"gitlab.com/gigaming/igaming/serverless/go-match-queue/app"
)

func main() {
	app := app.NewApp(time.Duration(time.Second))
	app.StartApplication()
}
