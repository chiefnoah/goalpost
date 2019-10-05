package queue

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

//JobStatus is a enumerated int representing the processing status of a Job
type JobStatus int

// JobStatus types
const (
	Ack JobStatus = iota + 1
	Uack
	Nack
	Failed
)

//Job wraps arbitrary data for processing
type Job struct {
	Status     JobStatus
	ID         uint64
	Data       []byte
	RetryCount int
	Message    string
}

//DecodeJob decodes a gob encoded byte array into a Job struct and returns a pointer to it
func DecodeJob(b []byte) *Job {
	//TODO: this should return an error in the event decoder.Decode returns an err
	buffer := bytes.NewReader(b)
	decoder := gob.NewDecoder(buffer)
	job := Job{}
	decoder.Decode(&job)
	return &job
}

//Bytes returns a gob encoded byte array representation of *j
func (j *Job) Bytes() []byte {
	buffer := &bytes.Buffer{}
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(j)
	return buffer.Bytes()
}

//RecoverableWorkerError defines an error that a worker DoWork func
//can return that indicates the message should be retried
type RecoverableWorkerError struct {
	message string
}

func (e RecoverableWorkerError) Error() string {
	return fmt.Sprintf("Worker encountered a temporary error: %s", e.message)
}

//NewRecoverableWorkerError creates a new RecoverableWorkerError
func NewRecoverableWorkerError(message string) RecoverableWorkerError {
	return RecoverableWorkerError{message}
}
