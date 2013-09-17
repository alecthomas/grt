package grt

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
)

// JobQueueKeyer can be implemented by a type to specify a custom job queue key.
type JobQueueKeyer interface {
	JobQueueKey() []byte
}

// JobQueue is a basic Redis-based job queue.
type JobQueue struct {
	r     redis.Conn
	Queue string
}

// NewJobQueue creates a new Redis-based job queue. Jobs can be any
// JSON-encodable structure. Note that this currently relies on stable
// ordering of encoded objects.
func NewJobQueue(r redis.Conn, queue string) *JobQueue {
	return &JobQueue{r: r, Queue: queue}
}

// Cleanup should be called when a job runner starts up, to return any aborted
// in-progress jobs to the queue.
func (c *JobQueue) Cleanup() error {
	log.Print("Cleaning up in-progress jobs")
	// Move in-progress items back to queue
	for {
		v, err := c.r.Do("RPOPLPUSH", c.Queue+":processing", c.Queue)
		if err != nil {
			c.r.Close()
			return err
		}
		if v == nil {
			break
		}
		log.Printf("Moved %s from processing to waiting", v)
	}
	return nil
}

// IsQueued checks whether a job is currently queued for processing, or in-progress.
func (c *JobQueue) IsQueued(job interface{}) (bool, error) {
	ej, err := json.Marshal(job)
	if err != nil {
		return false, err
	}
	v, err := redis.Int(c.r.Do("SISMEMBER", c.Queue+":index", ej))
	if err != nil {
		return false, err
	}
	return v != 0, nil
}

// Submit a job for processing.
func (c *JobQueue) Submit(job interface{}) error {
	ej, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if queued, err := c.IsQueued(job); err != nil || queued {
		return fmt.Errorf("Repository %s already queued", job)
	}
	c.r.Send("MULTI")
	c.r.Send("SADD", c.Queue+":index", ej)
	c.r.Send("LPUSH", c.Queue, ej)
	_, err = c.r.Do("EXEC")
	return err
}

// Get some work.
func (c *JobQueue) Get(v interface{}) (*Work, error) {
	job, err := redis.Bytes(c.r.Do("BRPOPLPUSH", c.Queue, c.Queue+":processing", 0))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(job, v)
	work := &Work{c.r, c.Queue, job}
	if err != nil {
		if cerr := work.Resubmit(); cerr != nil {
			return nil, cerr
		}
		return nil, err
	}
	return work, nil
}

// Work represents an in-progress job. Complete() or Resubmit() *must* be called
// after processing or a recoverable error occurs, respectively.
type Work struct {
	r     redis.Conn
	Queue string
	key   []byte
}

func (w *Work) String() string {
	return fmt.Sprintf("%s:%s", w.Queue, w.key)
}

// Complete a job and remove it from the in-progress queue.
func (w *Work) Complete() error {
	w.r.Send("MULTI")
	w.r.Send("LREM", w.Queue+":processing", 0, w.key)
	w.r.Send("SREM", w.Queue+":index", w.key)
	_, err := w.r.Do("EXEC")
	return err
}

// Resubmit a job and return it to the job queue.
func (w *Work) Resubmit() error {
	w.r.Send("MULTI")
	w.r.Send("LREM", w.Queue+":processing", 0, w.key)
	w.r.Send("LPUSH", w.Queue, w.key)
	_, err := w.r.Do("EXEC")
	return err
}

func (w *Work) marshal(job interface{}) (key []byte, payload []byte, err error) {
	payload, err = json.Marshal(job)
	if err != nil {
		return nil, nil, err
	}
	key = payload
	if keyer, ok := job.(JobQueueKeyer); ok {
		payload = keyer.JobQueueKey()
	}
	return
}

func (w *Work) unmarshal(payload []byte, v interface{}) error {
	return json.Unmarshal(payload, v)
}
