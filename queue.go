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
	ID            string
	db            *bolt.DB
	notifier      chan uint64
	workers       []*Worker
	shutdownFuncs []context.CancelFunc
	wg            *sync.WaitGroup
}

//Init creates a connection to the internal database and initializes the Queue type
func Init(filepath string) (*Queue, error) {
	q := &Queue{ID: filepath}
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
	err = q.resumeUnackedJobs()
	if err != nil {
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
				err := q.UpdateJobStatus(jobID, Uack, fmt.Sprintf("Picked up by %s", w.ID()))
				if err != nil {
					log.Printf("Unable to update job status: %s", err)
					continue
				}
				//If subsequent calls to UpdateJobStatus fail, the whole thing is probably hosed and
				//it should probably do something more drastic for error handling.
				job, err := q.GetJobByID(jobID)
				if err != nil {
					log.Printf("Error processing job: %s", err)
					q.UpdateJobStatus(jobID, Failed, err.Error())
					continue
				}
				// Call the worker func handling this job
				err = w.DoWork(ctx, job)
				if err != nil {
					_, ok := err.(RecoverableWorkerError)
					if ok {
						//temporary error, retry
						log.Printf("Received temporary error: %s. Retrying...", err.Error())
						q.UpdateJobStatus(jobID, Nack, err.Error())
					} else {
						log.Printf("Permanent error received from worker: %s", err)
						//permanent error, mark as failed
						q.UpdateJobStatus(jobID, Failed, err.Error())
					}
				} else {
					q.UpdateJobStatus(jobID, Ack, "Complete")
				}
				log.Printf("Finished processing job %d", jobID)
			default:
				//log.Printf("Worker: %s. No message to queue. Sleeping 500ms", w.ID())
				time.Sleep(500 * time.Millisecond)
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
		err := b.Put(intToByteArray(jobID), j.Bytes())
		return err
	})
	if err != nil {
		log.Printf("Unable to push job to queue: %s", err)
		return 0, err
	}
	q.notifier <- jobID
	return jobID, nil
}

//GetJobByID returns a pointer to a Job based on the primary key identifier id
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
		job = DecodeJob(jobBytes)
		return nil
	})
	return job, err
}

//UpdateJobStatus updates the processing status of a job
func (q *Queue) UpdateJobStatus(id uint64, status JobStatus, message string) error {
	err := q.db.Update(func(tx *bolt.Tx) error {
		jobsBucket := tx.Bucket([]byte(jobsBucketName))
		completedJobsBucket := tx.Bucket([]byte(completedJobsBucketName))
		jobBytes := jobsBucket.Get(intToByteArray(id))
		job := DecodeJob(jobBytes)
		job.Status = status
		job.Message = message
		// Move the job to the "completed" bucket if it's failed or Acked
		if status == Ack || status == Failed {
			log.Printf("Job is complete or failed. Moving to completed jobs bucket")
			err := completedJobsBucket.Put(intToByteArray(id), job.Bytes())
			if err != nil {
				log.Printf("Unable to add job %d to completedJobsBucket: ", err)
				// Don't return this, it's non-fatal
			}
			// remove the job from the 'active' bucket
			err = jobsBucket.Delete(intToByteArray(id))
			if err != nil {
				log.Printf("Unable to remove job from activie jobs bucket: %s", err)
			}
		} else if status == Nack {
			job.RetryCount = job.RetryCount + 1
			log.Printf("Nack: retry count: %d", job.RetryCount)
			err := jobsBucket.Put(intToByteArray(id), job.Bytes())
			if err != nil {
				log.Printf("Unable to update job in active jobs bucket: %s", err)
				return err
			}
		} else {
			err := jobsBucket.Put(intToByteArray(id), job.Bytes())
			if err != nil {
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
