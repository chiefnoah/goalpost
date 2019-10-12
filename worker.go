package goalpost

import "context"

//Worker represents a worker for handling Jobs
type Worker interface {
	//DoWork is called when a worker picks up a job from the queue
	//Context can be used for cancelling jobs early when Close
	//is called on the Queue
	DoWork(context.Context, *Job) error
	//ID is a semi-unique identifier for a worker
	//it is primarily used for logging purposes
	ID() string
}
