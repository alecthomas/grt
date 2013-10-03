package grt

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
)

var (
	// ErrAlreadyQueued is returned by Submit() when a duplicate job is submitted.
	ErrAlreadyQueued = errors.New("job already queued")
)

// JobQueueKeyer can be implemented by a type to specify a custom job queue key.
type JobQueueKeyer interface {
	JobQueueKey() []byte
}

// JobQueue is a basic Redis-based job queue. Not thread-safe.
type JobQueue struct {
	pool  *redis.Pool
	Queue string
}

// NewJobQueue creates a new Redis-based job queue. Jobs can be any
// JSON-encodable structure. Note that this currently relies on stable
// ordering of encoded objects.
func NewJobQueue(pool *redis.Pool, queue string) *JobQueue {
	return &JobQueue{pool: pool, Queue: queue}
}

// Cleanup should be called when a job runner starts up, to return any aborted
// in-progress jobs to the queue.
func (c *JobQueue) Cleanup() error {
	r := c.pool.Get()
	defer r.Close()
	log.Printf("Cleaning up in-progress jobs in %s", c.Queue)
	// Move in-progress items back to queue
	for {
		v, err := r.Do("RPOPLPUSH", c.Queue+":processing", c.Queue)
		if err != nil {
			r.Close()
			return err
		}
		if v == nil {
			break
		}
		log.Printf("Moved %s from processing to waiting", v)
	}
	return nil
}

// Len returns the length of the queue.
func (c *JobQueue) Len() (int, error) {
	r := c.pool.Get()
	defer r.Close()
	l, err := redis.Int(r.Do("HLEN", c.Queue+":payload"))
	if err == redis.ErrNil {
		return 0, nil
	}
	return l, err
}

// IsQueued checks whether a job is currently queued for processing, or in-progress.
func (c *JobQueue) IsQueued(job interface{}) (bool, error) {
	r := c.pool.Get()
	defer r.Close()
	key, _, err := jobQueueMarshal(job)
	if err != nil {
		return false, err
	}
	v, err := redis.Int(r.Do("HEXISTS", c.Queue+":payload", key))
	if err != nil {
		return false, err
	}
	return v != 0, nil
}

// Submit a job for processing.
func (c *JobQueue) Submit(job interface{}) error {
	r := c.pool.Get()
	defer r.Close()
	key, payload, err := jobQueueMarshal(job)
	if queued, err := c.IsQueued(job); err != nil || queued {
		return ErrAlreadyQueued
	}

	r.Send("MULTI")
	r.Send("LPUSH", c.Queue, key)
	r.Send("HSET", c.Queue+":payload", key, payload)
	_, err = r.Do("EXEC")
	return err
}

// Get some work.
func (c *JobQueue) Get(v interface{}) (*Work, error) {
	r := c.pool.Get()
	defer r.Close()
	key, err := redis.Bytes(r.Do("BRPOPLPUSH", c.Queue, c.Queue+":processing", 0))
	if err != nil {
		return nil, err
	}
	d, err := redis.Bytes(r.Do("HGET", c.Queue+":payload", key))
	if err == nil {
		err = jobQueueUnmarshal(d, v)
	}
	work := &Work{pool: c.pool, Queue: c.Queue, key: key}
	if err != nil {
		if rerr := work.Resubmit(); rerr != nil {
			panic("could not resubmit job: " + rerr.Error())
		}
		return nil, err
	}
	return work, nil
}

// Work represents an in-progress job. Complete() or Resubmit() *must* be called
// after processing or a recoverable error occurs, respectively.
type Work struct {
	pool  *redis.Pool
	Queue string
	key   []byte
}

func (w *Work) String() string {
	return fmt.Sprintf("%s:%s", w.Queue, w.key)
}

// Complete a job and remove it from the in-progress queue. Concurrency safe.
func (w *Work) Complete() error {
	r := w.pool.Get()
	defer r.Close()
	r.Send("MULTI")
	r.Send("LREM", w.Queue+":processing", 0, w.key)
	r.Send("HDEL", w.Queue+":payload", w.key)
	_, err := r.Do("EXEC")
	return err
}

// Resubmit a job and return it to the job queue. Concurrency safe.
func (w *Work) Resubmit() error {
	r := w.pool.Get()
	defer r.Close()
	r.Send("MULTI")
	r.Send("LREM", w.Queue+":processing", 0, w.key)
	r.Send("LPUSH", w.Queue, w.key)
	_, err := r.Do("EXEC")
	return err
}

func jobQueueRawMarshal(v interface{}) (payload []byte, err error) {
	payload, err = json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return
}

func jobQueueMarshal(job interface{}) (key []byte, payload []byte, err error) {
	if payload, err = jobQueueRawMarshal(job); err != nil {
		return
	}
	key = payload
	if keyer, ok := job.(JobQueueKeyer); ok {
		payload = keyer.JobQueueKey()
	}
	return
}

func jobQueueUnmarshal(payload []byte, v interface{}) error {
	return json.Unmarshal(payload, v)
}
