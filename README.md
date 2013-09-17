# Go Redis Toolbox

Lightweight implementations of some useful Redis patterns.

## Lock

```go
lock := grt.NewLock(r, "lock")
err := lock.Lock()
if err != nil {
    panic(err)
}
defer lock.Unlock()

// Do work
```

Also supports `LockWait(timeout)`, a non-blocking lock.

## Job Queue

### Producer

```go
jobs := grt.NewJobQueue(r, "jobs")

for url := range urlsToFetch {
    err := jobs.Submit(url)
    if err != nil {
        panic(err)
    }
}
```

### Consumer

```go
jobs := grt.NewJobQueue(r, "jobs")

var url *string

for {
    // Get a URL to process
    handle, err := jobs.Get(url)
    if err != nil {
        panic(err)
    }

    // Do the work
    err = fetchUrl(url)

    // Resubmit the job if the work failed
    if err != nil {
        handle.Resubmit()
        panic(err)
    }

    // Mark job as complete
    err = handle.Complete()
    if err != nil {
        panic(err)
    }
}
```
