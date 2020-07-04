package goalpost

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
)

type testWorker struct{}

type workArgs struct {
	ctx context.Context
	job Job
}

type longWorker struct{}

var workChan = make(chan workArgs)

func (w *testWorker) DoWork(ctx context.Context, j *Job) error {
	fmt.Printf("DoWork called: %+v", *j)
	//time.Sleep(1 * time.Second)
	jc := *j
	args := workArgs{ctx: ctx, job: jc}
	workChan <- args
	return nil
}

func (w *testWorker) ID() string {
	return "testWorker"
}

func (w *longWorker) DoWork(ctx context.Context, j *Job) error {
	for i := 0; i < 100; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}
func (w *longWorker) ID() string {
	return "longWorker"
}

func TestQueueInit(t *testing.T) {
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	q.db.View(func(tx *bolt.Tx) error {
		jobBucket := tx.Bucket([]byte(jobsBucketName))
		assert.NotNil(t, jobBucket)

		completedJobsBucket := tx.Bucket([]byte(completedJobsBucketName))
		assert.NotNil(t, completedJobsBucket)

		return nil
	})

	assert.NotNil(t, q.notifier)
	assert.Equal(t, make([]*Worker, 0), q.workers)
}

func TestRegisterWorker(t *testing.T) {
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	worker := &testWorker{}
	var workerInterface Worker
	q.RegisterWorker(worker)

	expectedWorkerList := make([]*Worker, 1)
	workerInterface = worker
	expectedWorkerList[0] = &workerInterface
	assert.Equal(t, expectedWorkerList, q.workers)

}

func TestPushJob(t *testing.T) {
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	worker := &testWorker{}

	q.RegisterWorker(worker)
	testData := []byte("abcd")
	t.Logf("Pushign test job onto queue")
	q.PushBytes(testData)
	args := <-workChan
	assert.NotNil(t, args)

	//A read-write transaction that does nothing, so we can ensure the trasaction for
	// marking the job as failed is committed before continuing
	q.db.Update(func(tx *bolt.Tx) error {
		//Anti-race condition transaction 🏃‍♀
		return nil
	})

	completedJob, err := q.getJobInBucketByID(args.job.ID, completedJobsBucketName)
	assert.Nil(t, err)
	assert.NotNil(t, completedJob)
	assert.Equal(t, Ack, completedJob.Status)
}

func TestGracefulShutdown(t *testing.T) {
	//This test will take over 30s to complete if the early exit doesn't work
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	worker := &longWorker{}

	q.RegisterWorker(worker)
	testData := []byte("abcd")

	q.PushBytes(testData)
	time.Sleep(5 * time.Second)
	q.Close()
}

var erroringChan = make(chan Job)

type erroringWorker struct{}

func (w *erroringWorker) DoWork(ctx context.Context, j *Job) error {
	log.Printf("Recevied job %d that's been retried %d times", j.ID, j.RetryCount)
	if j.RetryCount < 2 {
		return NewRecoverableWorkerError("test error")
	} else {
		var newJob Job
		newJob = Job(*j)
		erroringChan <- newJob
		return errors.New("Fatal error")
	}
}
func (w *erroringWorker) ID() string {
	return "erroringWorker"
}
func TestErroringJob(t *testing.T) {
	//This tests that a recoverable error is retried,
	// and that a non-recoverable error is marked as failed
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	worker := &erroringWorker{}
	q.RegisterWorker(worker)

	testData := []byte("abcd")
	q.PushBytes(testData)
	job := <-erroringChan

	assert.NotNil(t, job)
	assert.Equal(t, 2, job.RetryCount)

	//A read-write transaction that does nothing, so we can ensure the trasaction for
	// marking the job as failed is committed before continuing
	q.db.Update(func(tx *bolt.Tx) error {
		//Anti-race condition transaction 🏃
		return nil
	})

	//The job shouldn't exist in the active jobs bucket anymore
	oldJob, err := q.getJobInBucketByID(job.ID, jobsBucketName)
	assert.Nil(t, oldJob)
	completedJob, err := q.getJobInBucketByID(job.ID, completedJobsBucketName)
	assert.Nil(t, err)
	assert.Equal(t, Failed, completedJob.Status)
}

func TestResumeNackedJobs(t *testing.T) {
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	worker := &testWorker{}

	q.RegisterWorker(worker)
	testData := []byte("abcd")

	//q.PushBytes(&testData)
	var jobID uint64
	//Put a Nacked job on the queue
	q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(jobsBucketName))
		jobID, _ = b.NextSequence()
		job := &Job{
			ID:         jobID,
			Status:     Nack,
			Data:       testData,
			RetryCount: 0,
		}
		err := b.Put(intToByteArray(jobID), job.Bytes())
		return err
	})
	t.Log("Resuming stopped jobs")
	q.resumeUnackedJobs()

	args := <-workChan
	t.Log(args.job.ID)
	assert.NotNil(t, args)

	//A read-write transaction that does nothing, so we can ensure the trasaction for
	// marking the job as failed is committed before continuing
	q.db.Update(func(tx *bolt.Tx) error {
		//Anti-race condition transaction 🏃‍♀
		return nil
	})

	completedJob, err := q.getJobInBucketByID(args.job.ID, completedJobsBucketName)
	assert.Nil(t, err)
	assert.NotNil(t, completedJob)
	assert.Equal(t, Ack, completedJob.Status)
}

func TestResumeUackedJobs(t *testing.T) {
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()

	worker := &testWorker{}

	q.RegisterWorker(worker)
	testData := []byte("abcd")

	//q.PushBytes(&testData)
	var jobID uint64
	//Put a Nacked job on the queue
	q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(jobsBucketName))
		jobID, _ = b.NextSequence()
		job := &Job{
			ID:         jobID,
			Status:     Uack,
			Data:       testData,
			RetryCount: 0,
		}
		err := b.Put(intToByteArray(jobID), job.Bytes())
		return err
	})
	t.Log("Resuming stopped jobs")
	q.resumeUnackedJobs()

	args := <-workChan
	t.Log(args.job.ID)
	assert.NotNil(t, args)

	//A read-write transaction that does nothing, so we can ensure the trasaction for
	// marking the job as failed is committed before continuing
	q.db.Update(func(tx *bolt.Tx) error {
		//Anti-race condition transaction 🏃‍♀
		return nil
	})

	completedJob, err := q.getJobInBucketByID(args.job.ID, completedJobsBucketName)
	assert.Nil(t, err)
	assert.NotNil(t, completedJob)
	assert.Equal(t, Ack, completedJob.Status)
}

func TestCleanOldJobs(t *testing.T) {
	os.Remove("test.db")
	q, err := Init("test")
	assert.Nil(t, err)
	defer os.Remove("test.db")
	defer q.Close()
	t.Log("Adding test failed jobs")
	// Set up some failed jobs
	for _, v := range "abcdefghijklmnopqrstuvwxyz" {
		j := &Job{
			Status:     Failed,
			Data:       []byte{byte(v)},
			RetryCount: 0,
		}
		err := q.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(completedJobsBucketName))
			jobID, _ := b.NextSequence()
			j.ID = jobID
			return b.Put(intToByteArray(jobID), j.Bytes())
		})
		assert.Nil(t, err)
	}
	t.Log("Checking number of failed jobs is 26")
	// Check that jobs are actually there
	q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(completedJobsBucketName))
		stats := b.Stats()
		assert.Equal(t, stats.KeyN, 26)
		return nil
	})
	q.CleanOldJobs()
	t.Log("Checking all failed/completed jobs are deleted")
	q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(completedJobsBucketName))
		stats := b.Stats()
		assert.Equal(t, stats.KeyN, 0)
		return nil
	})
}
