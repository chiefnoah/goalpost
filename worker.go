package queue

import "context"

//Worker represents a worker for handling Jobs
type Worker interface {
	DoWork(context.Context, *Job) error
	ID() string
}

//WorkerConfig configures a worker
type WorkerConfig struct {
	//PollingRate is the number of milliseconds to sleep after checking the queue for a job
	PollingRate uint8
}
