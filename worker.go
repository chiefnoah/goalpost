package goalpost

import "context"

//Worker represents a worker for handling Jobs
type Worker interface {
	DoWork(context.Context, *Job) error
	ID() string
}
