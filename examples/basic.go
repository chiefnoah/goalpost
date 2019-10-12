package main

import (
	"context"
	"fmt"
	"time"
)
import "github.com/chiefnoah/goalpost"

const eventQueueID = "event-queue"

//Define a type that implements the goalpost.Worker interface
type worker struct {
	id string
}

func (w *worker) ID() string {
	return w.id
}

func (w *worker) DoWork(ctx context.Context, j *goalpost.Job) error {
	//do something cool!
	fmt.Printf("Hello, %s\n", j.Data)
	//Something broke, but we should retry it...
	if j.RetryCount < 9 { //try 10 times
		return goalpost.NewRecoverableWorkerError("Something broke, try again")
	}

	//Something *really* broke, don't retry
	//return errors.New("Something broke, badly")

	//Everything's fine, we're done here
	return nil
}

func main() {

	//Init a queue
	q, _ := goalpost.Init(eventQueueID)
	//remember to handle your errors :)

	//Create a worker with id "worker-id"
	w := &worker{
		id: "worker-1",
	}
	//register the worker, so it can do work
	q.RegisterWorker(w)

	//Let's do some work...
	q.PushBytes([]byte("World"))
	//You should see "Hello, World" printed 10 times

	//Make sure your process doesn't exit before your workers can do their work
	time.Sleep(10 * time.Second)

	//Remember to close your queue when you're done using it
	q.Close()
}
