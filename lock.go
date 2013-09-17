package grt

import (
	"errors"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

var (
	// LockTimeout is returned by LockWait() when the lock expires.
	LockTimeout = errors.New("lock timeout")
)

// Lock is a Redis-based lock.
type Lock struct {
	r   redis.Conn
	Key string
	// Set the expiry time.
	Expiry  time.Duration
	lock    sync.Mutex
	errors  chan error
	stop    chan bool
	stopped chan bool
}

// NewLock creates a new Redis lock.
func NewLock(r redis.Conn, key string) *Lock {
	return &Lock{
		r:       r,
		Key:     key,
		Expiry:  time.Second * 2,
		errors:  make(chan error, 1),
		stop:    make(chan bool, 1),
		stopped: make(chan bool, 1),
	}
}

// Lock is a blocking lock. Returns nil if the lock is acquired, or any Redis error.
func (l *Lock) Lock() error {
	return l.LockWait(time.Hour * 100000)
}

// LockWait is a non-blocking lock. Returns nil if the lock is acquired,
// LockTimeout if the timeout is reached, or any Redis error.
func (l *Lock) LockWait(wait time.Duration) error {
	l.lock.Lock()
	expire := time.Now().Add(wait)
	for {
		v, err := l.r.Do("SET", l.Key, 1, "NX", "PX", l.Expiry.Nanoseconds()/1000000)
		if err != nil {
			l.lock.Unlock()
			return err
		}
		if v != nil {
			break
		}

		time.Sleep(l.Expiry)
		if time.Now().After(expire) {
			l.lock.Unlock()
			return LockTimeout
		}
	}

	// Lock heartbeat.
	go l.heartbeat()
	return nil
}

func (l *Lock) heartbeat() {
	wait := time.Tick(l.Expiry / 4)
	for {
		_, err := l.r.Do("SET", l.Key, 1, "XX", "PX", l.Expiry.Nanoseconds()/1000000)
		if err != nil {
			l.errors <- err
			return
		}

		select {
		case <-l.stop:
			l.stopped <- true
			return
		case <-wait:
		}
	}
}

// Unlock the lock.
func (l *Lock) Unlock() {
	defer l.lock.Unlock()
	l.stop <- true
	<-l.stopped
}
