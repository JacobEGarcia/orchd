// orchd: a small job queue / scheduler with a worker pool, retries with
// exponential backoff, a dead-letter list, and heartbeat health checks that
// requeue work from hung workers. Single process, in-memory state.
package main

import (
	"bufio"
	"container/heap"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// hangHook lets tests decide deterministically whether an attempt hangs.
var hangHook func(*Job) bool

type Job struct {
	ID       int64   `json:"id"`
	WorkMs   int     `json:"work_ms"`
	FailProb float64 `json:"fail_prob"` // injected failure probability per attempt
	HangProb float64 `json:"hang_prob"` // injected probability a worker hangs on this attempt
	CPUIters int     `json:"cpu_iters"` // real CPU work: this many chained SHA-256 rounds
	Attempts int     `json:"attempts"`
	State    string  `json:"state"` // queued, running, done, dead
	readyAt  time.Time
	epoch    int64 // bumped on requeue so stale workers cannot complete the job
}

type pq []*Job

func (p pq) Len() int            { return len(p) }
func (p pq) Less(i, j int) bool  { return p[i].readyAt.Before(p[j].readyAt) }
func (p pq) Swap(i, j int)       { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x interface{}) { *p = append(*p, x.(*Job)) }
func (p *pq) Pop() interface{} {
	o := *p
	n := len(o)
	x := o[n-1]
	*p = o[:n-1]
	return x
}

type worker struct {
	id       int
	lastBeat atomic.Int64 // unix nano
	job      *Job
	jobEpoch int64
	healthy  bool
}

type Server struct {
	mu                            sync.Mutex
	cond                          *sync.Cond
	q                             pq
	jobs                          map[int64]*Job
	workers                       []*worker
	nextID                        int64
	maxAttempts                   int
	baseBackoff                   time.Duration
	hbTimeout                     time.Duration
	healthCheck                   bool
	retries                       bool
	targetWorkers                 int // replacements are only spawned while healthy workers < target
	done, dead, retried, requeued int64
	wal                           *os.File
	walMu                         sync.Mutex
	walSync                       bool
}

type walRec struct {
	Op  string `json:"op"` // sub, done, dead
	Job *Job   `json:"job,omitempty"`
	ID  int64  `json:"id,omitempty"`
}

// logWAL appends one record. For submissions it fsyncs when walSync is set so an
// acknowledged job survives a crash. done/dead records are not fsynced: losing one
// only causes the job to run again after recovery (at-least-once).
func (s *Server) logWAL(r walRec, sync bool) {
	if s.wal == nil {
		return
	}
	b, _ := json.Marshal(r)
	s.walMu.Lock()
	s.wal.Write(append(b, '\n'))
	if sync && s.walSync {
		s.wal.Sync()
	}
	s.walMu.Unlock()
}

// recoverWAL replays the log and requeues every job that has no done/dead record.
func (s *Server) recoverWAL(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	pending := map[int64]*Job{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r walRec
		if json.Unmarshal(sc.Bytes(), &r) != nil {
			continue // torn final line after a crash
		}
		switch r.Op {
		case "sub":
			pending[r.Job.ID] = r.Job
			if r.Job.ID > s.nextID {
				s.nextID = r.Job.ID
			}
		case "done", "dead":
			delete(pending, r.ID)
		}
	}
	for _, j := range pending {
		j.State, j.Attempts, j.readyAt = "queued", 0, time.Now()
		s.jobs[j.ID] = j
		heap.Push(&s.q, j)
	}
	return len(pending), nil
}

func (s *Server) enqueue(j *Job) {
	s.mu.Lock()
	s.nextID++
	j.ID = s.nextID
	j.State = "queued"
	j.readyAt = time.Now()
	s.jobs[j.ID] = j
	s.mu.Unlock()
	s.logWAL(walRec{Op: "sub", Job: j}, true) // durable before it is queued or acknowledged
	s.mu.Lock()
	heap.Push(&s.q, j)
	s.mu.Unlock()
	s.cond.Signal()
}

// next blocks until a job is ready.
func (s *Server) next(w *worker) (*Job, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if len(s.q) > 0 {
			d := time.Until(s.q[0].readyAt)
			if d <= 0 {
				j := heap.Pop(&s.q).(*Job)
				j.State = "running"
				j.Attempts++
				w.job, w.jobEpoch = j, j.epoch
				return j, j.epoch
			}
			// wake when the head becomes ready
			go func(d time.Duration) { time.Sleep(d); s.cond.Broadcast() }(d)
		}
		s.cond.Wait()
	}
}

func (s *Server) finish(w *worker, j *Job, epoch int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j.epoch != epoch || j.State != "running" { // requeued by health checker: drop stale result
		return
	}
	w.job = nil
	if ok {
		j.State = "done"
		s.done++
		s.logWAL(walRec{Op: "done", ID: j.ID}, false)
		return
	}
	if !s.retries || j.Attempts >= s.maxAttempts {
		j.State = "dead"
		s.dead++
		s.logWAL(walRec{Op: "dead", ID: j.ID}, false)
		return
	}
	s.retried++
	back := s.baseBackoff << uint(j.Attempts-1)
	back += time.Duration(rand.Int63n(int64(back)/2 + 1)) // jitter
	j.State = "queued"
	j.readyAt = time.Now().Add(back)
	heap.Push(&s.q, j)
	s.cond.Broadcast()
}

func (s *Server) runWorker(w *worker) {
	for {
		j, ep := s.next(w)
		w.lastBeat.Store(time.Now().UnixNano())
		hang := rand.Float64() < j.HangProb
		if hangHook != nil {
			hang = hangHook(j)
		}
		if hang {
			// simulate a hung worker: stops heartbeating for good
			select {}
		}
		if j.CPUIters > 0 {
			h := sha256.Sum256([]byte("seed"))
			for i := 0; i < j.CPUIters; i++ {
				h = sha256.Sum256(h[:])
				if i%2000 == 0 {
					w.lastBeat.Store(time.Now().UnixNano()) // heartbeat inside long CPU work
				}
			}
		}
		// do work in slices so we can heartbeat
		left := time.Duration(j.WorkMs) * time.Millisecond
		for left > 0 {
			step := 10 * time.Millisecond
			if left < step {
				step = left
			}
			time.Sleep(step)
			left -= step
			w.lastBeat.Store(time.Now().UnixNano())
		}
		s.finish(w, j, ep, rand.Float64() >= j.FailProb)
	}
}

// healthLoop marks workers unhealthy when heartbeats stop, requeues their job,
// and spawns a replacement worker.
func (s *Server) healthLoop() {
	for range time.Tick(s.hbTimeout / 4) {
		if !s.healthCheck {
			continue
		}
		s.mu.Lock()
		var spawn []*worker
		for _, w := range s.workers {
			if !w.healthy {
				continue
			}
			if time.Since(time.Unix(0, w.lastBeat.Load())) > s.hbTimeout && w.job != nil {
				w.healthy = false
				j := w.job
				w.job = nil
				j.epoch++
				s.requeued++
				j.State = "queued"
				j.readyAt = time.Now()
				heap.Push(&s.q, j)
				if s.healthyCount()+len(spawn) < s.targetWorkers { // replace capacity, but never grow past the target
					nw := &worker{id: len(s.workers) + len(spawn), healthy: true}
					nw.lastBeat.Store(time.Now().UnixNano())
					spawn = append(spawn, nw)
				}
			}
		}
		s.workers = append(s.workers, spawn...)
		s.mu.Unlock()
		for _, nw := range spawn {
			go s.runWorker(nw)
		}
		s.cond.Broadcast()
	}
}

// healthyCount must be called with s.mu held.
func (s *Server) healthyCount() int {
	n := 0
	for _, w := range s.workers {
		if w.healthy {
			n++
		}
	}
	return n
}

func (s *Server) stats() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	healthy := 0
	for _, w := range s.workers {
		if w.healthy {
			healthy++
		}
	}
	return map[string]interface{}{"submitted": s.nextID, "done": s.done, "dead": s.dead, "retried": s.retried,
		"requeued_by_healthcheck": s.requeued, "queued": len(s.q), "workers_total": len(s.workers), "workers_healthy": healthy}
}

func main() {
	addr := flag.String("addr", ":8080", "listen")
	n := flag.Int("workers", 8, "worker count")
	maxA := flag.Int("max-attempts", 5, "attempts before dead-letter")
	retries := flag.Bool("retries", true, "retry failed jobs")
	hc := flag.Bool("healthcheck", true, "requeue jobs from hung workers")
	walPath := flag.String("wal", "", "write-ahead log path (empty = in-memory only)")
	walSync := flag.Bool("wal-sync", true, "fsync each submission")
	hb := flag.Duration("hb-timeout", 200*time.Millisecond, "heartbeat timeout")
	flag.Parse()
	s := &Server{jobs: map[int64]*Job{}, maxAttempts: *maxA, baseBackoff: 10 * time.Millisecond,
		hbTimeout: *hb, healthCheck: *hc, retries: *retries, targetWorkers: *n}
	s.cond = sync.NewCond(&s.mu)
	if *walPath != "" {
		n, err := s.recoverWAL(*walPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("recovered %d unfinished jobs from %s", n, *walPath)
		f, err := os.OpenFile(*walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		s.wal, s.walSync = f, *walSync
	}
	for i := 0; i < *n; i++ {
		w := &worker{id: i, healthy: true}
		w.lastBeat.Store(time.Now().UnixNano())
		s.workers = append(s.workers, w)
		go s.runWorker(w)
	}
	go s.healthLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", func(rw http.ResponseWriter, r *http.Request) {
		var j Job
		if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
			http.Error(rw, err.Error(), 400)
			return
		}
		s.enqueue(&j)
		json.NewEncoder(rw).Encode(map[string]int64{"id": j.ID})
	})
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, r *http.Request) { rw.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, r *http.Request) {
		if s.stats()["workers_healthy"].(int) == 0 {
			http.Error(rw, "no healthy workers", 503)
			return
		}
		rw.Write([]byte("ready"))
	})
	mux.HandleFunc("/stats", func(rw http.ResponseWriter, r *http.Request) { json.NewEncoder(rw).Encode(s.stats()) })
	log.Printf("orchd on %s workers=%d retries=%v healthcheck=%v", *addr, *n, *retries, *hc)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
