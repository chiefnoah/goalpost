package goalpost

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	jobsBucketName          = "Jobs"
	completedJobsBucketName = "CompletedJobs"
)

// Queue represents a queue
type Queue struct {
	//ID is a unique identifier for a Queue
	ID string
	//db represents a handle to a bolt db
	db *bolt.DB
	//notifier is a chan used to signal workers there is a job to begin working
	notifier chan uint64
	//workeres is a list of *Workers
	workers []*Worker
	//shutdownFuncs are context.CancleFuncs used to signal graceful shutdown
	shutdownFuncs []context.CancelFunc
	//wg is used to help gracefully shutdown workers
	wg *sync.WaitGroup

	//PollRate the duration to Sleep each worker before checking the queue for jobs again
	//queue for jobs again.
	//Default: 500 milliseconds
	PollRate time.Duration
}

//Init creates a connection to the internal database and initializes the Queue type
//filepath must be a valid path to a file. It cannot be shared between instances of
//a Queue. If the  file cannot be opened r/w, an error is returned.
func Init(filepath string) (*Queue, error) {
	q := &Queue{ID: filepath, PollRate: time.Duration(500 * time.Millisecond)}
	db, err := bolt.Open(filepath+".db", 0600, nil)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	q.db = db

	//Create bucket for jobs if it doesn't already exist
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(jobsBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		// A second bucket so we can move completed jobs to a "archived" bucket
		// to prevent unecessary scanning of old jobs when resuming from previously used queue
		_, err = tx.CreateBucketIfNotExists([]byte(completedJobsBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	// Make notification channels
	c := make(chan uint64, 1000) //TODO: channel probably isn't the best way to handle the queue buffer
	q.notifier = c
	q.workers = make([]*Worker, 0)
	q.shutdownFuncs = make([]context.CancelFunc, 0)
	var wg sync.WaitGroup
	q.wg = &wg
	//resume stopped jobs
	if err = q.resumeUnackedJobs(); err != nil {
		log.Printf("Unable to resume jobs from bucket: %s", err)
		//Don't fail out, this isn't really fatal. But maybe it should be?
	}
	return q, nil
}

//Close attempts to gracefull shutdown all workers in a queue and shutdown the db connection
func (q *Queue) Close() error {
	for _, f := range q.shutdownFuncs {
		f()
	}
	q.wg.Wait()
	q.notifier = nil
	q.workers = nil
	q.shutdownFuncs = nil
	return q.db.Close()
}

//registerWorkerWithContext contains the main loop for all Workers.
func (q *Queue) registerWorkerWithContext(ctx context.Context, w Worker) {
	q.workers = append(q.workers, &w)
	q.wg.Add(1)
	log.Printf("Registering worker with ID: %s", w.ID())
	//The big __main loop__ for workers.
	go func() {
		log.Printf("Starting up new worker...")
		var jobID uint64
		for {
			// receive a notification from the queue chan
			select {
			case <-ctx.Done():
				log.Printf("Received signal to shutdown worker. Exiting.")
				q.wg.Done()
				return
			case jobID = <-q.notifier:
				log.Printf("Receive job ID %d", jobID)
				if err := q.updateJobStatus(jobID, Uack, fmt.Sprintf("Picked up by %s", w.ID())); err != nil {
					log.Printf("Unable to update job status: %s", err)
					continue
				}
				//If subsequent calls to updateJobStatus fail, the whole thing is probably hosed and
				//it should probably do something more drastic for error handling.
				job, err := q.GetJobByID(jobID)
				if err != nil {
					log.Printf("Error processing job: %s", err)
					q.updateJobStatus(jobID, Failed, err.Error())
					continue
				}
				// Call the worker func handling this job
				err = w.DoWork(ctx, job)
				if err != nil {
					_, ok := err.(RecoverableWorkerError)
					if ok {
						//temporary error, retry
						log.Printf("Received temporary error: %s. Retrying...", err.Error())
						q.updateJobStatus(jobID, Nack, err.Error())
					} else {
						log.Printf("Permanent error received from worker: %s", err)
						//permanent error, mark as failed
						q.updateJobStatus(jobID, Failed, err.Error())
					}
				} else {
					q.updateJobStatus(jobID, Ack, "Complete")
				}
				log.Printf("Finished processing job %d", jobID)
			default:
				//log.Printf("Worker: %s. No message to queue. Sleeping 500ms", w.ID())
				time.Sleep(q.PollRate)
			}
		}
	}()
}

//RegisterWorker registers a Worker to handle queued Jobs
func (q *Queue) RegisterWorker(w Worker) {
	baseCtx := context.Background()
	ctx, cancelFunc := context.WithCancel(baseCtx)
	q.shutdownFuncs = append(q.shutdownFuncs, cancelFunc)
	q.registerWorkerWithContext(ctx, w)
}

//PushBytes wraps arbitrary binary data in a job and pushes it onto the queue
func (q *Queue) PushBytes(d []byte) (uint64, error) {
	job := &Job{
		Status:     Uack,
		Data:       d,
		RetryCount: 0,
	}
	return q.PushJob(job)
}

//PushJob pushes a job to the queue and notifies workers
// Job.ID is always overwritten
func (q *Queue) PushJob(j *Job) (uint64, error) {
	var jobID uint64
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(jobsBucketName))
		jobID, _ = b.NextSequence()
		j.ID = jobID
		log.Printf("Storing job %d for processing", jobID)
		return b.Put(intToByteArray(jobID), j.Bytes())
	})
	if err != nil {
		log.Printf("Unable to push job to queue: %s", err)
		return 0, err
	}
	q.notifier <- jobID
	return jobID, nil
}

//GetJobByID returns a pointer to a Job based on the primary key identifier id
//It first checks active jobs, if it doesn't find the bucket for active jobs
//it searches in the completed jobs bucket.
func (q *Queue) GetJobByID(id uint64) (*Job, error) {
	job, err := q.getJobInBucketByID(id, jobsBucketName)
	if err != nil {
		return nil, err
	}
	if job == nil {
		log.Printf("Job not found in active jobs bucket, checking complete")
		job, err = q.getJobInBucketByID(id, completedJobsBucketName)
	}
	return job, err
}

func (q *Queue) getJobInBucketByID(id uint64, bucketName string) (*Job, error) {
	var job *Job
	err := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New("Invalid bucket")
		}
		jobBytes := b.Get(intToByteArray(id))
		if jobBytes == nil {
			log.Printf("Unknown key: %d", id)
			return nil
		}
		job, err = DecodeJob(jobBytes)
		return err
	})
	return job, err
}

//updateJobStatus updates the processing status of a job
func (q *Queue) updateJobStatus(id uint64, status JobStatus, message string) error {
	err := q.db.Update(func(tx *bolt.Tx) error {
		jobsBucket := tx.Bucket([]byte(jobsBucketName))
		completedJobsBucket := tx.Bucket([]byte(completedJobsBucketName))
		jobBytes := jobsBucket.Get(intToByteArray(id))
		job := DecodeJob(jobBytes)
		job.Status = status
		job.Message = message
		// Move the job to the "completed" bucket if it's failed or Acked
		if status == Ack || status == Failed {
			s := "failed"
			if status == Ack {
				s = "complete"
			}
			log.Printf("Job is %s. Moving to completed jobs bucket", s)
			if err := completedJobsBucket.Put(intToByteArray(id), job.Bytes()); err != nil {
				log.Printf("Unable to add job %d to completedJobsBucket: ", err)
				// Don't return this, it's non-fatal
			}
			// remove the job from the 'active' bucket
			if err := jobsBucket.Delete(intToByteArray(id)); err != nil {
				log.Printf("Unable to remove job from activie jobs bucket: %s", err)
			}
		} else if status == Nack {
			job.RetryCount = job.RetryCount + 1
			log.Printf("Nack: retry count: %d", job.RetryCount)
			if err := jobsBucket.Put(intToByteArray(id), job.Bytes()); err != nil {
				log.Printf("Unable to update job in active jobs bucket: %s", err)
				return err
			}
		} else {
			if err := jobsBucket.Put(intToByteArray(id), job.Bytes()); err != nil {
				log.Printf("Unable to update job in active jobs bucket: %s", err)
				return err
			}
		}
		return nil
	})
	//Put this job back on the queue if it was a Nack
	if status == Nack && err == nil {
		q.notifier <- id
	}
	return err
}

// CleanOldJobs loops through all jobs marked as completed or failed and deletes them from the database
// Warning: this is destructive, that job data is definitely done if you call this function.
func (q *Queue) CleanOldJobs() error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(completedJobsBucketName))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})

}

//resumeUnackedJobs loops through all jobs in the main job bucket and sends
//channel notifications if any are uacked or nacked
func (q *Queue) resumeUnackedJobs() error {
	return q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(jobsBucketName))
		c := b.Cursor()
		j := &Job{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			buffer := bytes.NewBuffer(v)
			decoder := gob.NewDecoder(buffer)
			_, err := buffer.Write(v)
			if err != nil {
				log.Printf("Error writing bytes to buffer: %s", err)
				return err
			}
			decoder.Decode(j)
			if j.Status == Uack || j.Status == Nack {
				log.Printf("Job %d not processed. Retrying...", j.ID)
				q.notifier <- j.ID
			}
		}
		return nil
	})
}

//intToByteArray small helper function for dealing with bucket record IDs
func intToByteArray(i uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, i)
	return bs
}
