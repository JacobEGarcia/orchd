package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestServer(workers int, retries bool, maxA int) *Server {
	s := &Server{jobs: map[int64]*Job{}, maxAttempts: maxA, baseBackoff: time.Millisecond,
		hbTimeout: 100 * time.Millisecond, healthCheck: true, retries: retries, targetWorkers: workers}
	s.cond = sync.NewCond(&s.mu)
	for i := 0; i < workers; i++ {
		w := &worker{id: i, healthy: true}
		w.lastBeat.Store(time.Now().UnixNano())
		s.workers = append(s.workers, w)
		go s.runWorker(w)
	}
	go s.healthLoop()
	return s
}

func waitFor(t *testing.T, s *Server, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st := s.stats()
		if st["done"].(int64)+st["dead"].(int64) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout: %v", s.stats())
}

func TestAllJobsComplete(t *testing.T) {
	s := newTestServer(4, true, 3)
	for i := 0; i < 100; i++ {
		s.enqueue(&Job{WorkMs: 1})
	}
	waitFor(t, s, 100)
	if s.stats()["done"].(int64) != 100 {
		t.Fatal("not all done")
	}
}

func TestAlwaysFailingGoesDeadAfterMaxAttempts(t *testing.T) {
	s := newTestServer(2, true, 3)
	j := &Job{WorkMs: 1, FailProb: 1}
	s.enqueue(j)
	waitFor(t, s, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if j.State != "dead" || j.Attempts != 3 {
		t.Fatalf("state=%s attempts=%d", j.State, j.Attempts)
	}
}

func TestNoRetriesMeansDeadOnFirstFailure(t *testing.T) {
	s := newTestServer(2, false, 5)
	j := &Job{WorkMs: 1, FailProb: 1}
	s.enqueue(j)
	waitFor(t, s, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if j.Attempts != 1 || j.State != "dead" {
		t.Fatalf("attempts=%d state=%s", j.Attempts, j.State)
	}
}

func TestHungWorkerJobIsRequeuedAndCompletes(t *testing.T) {
	var once sync.Once
	hangHook = func(*Job) bool { h := false; once.Do(func() { h = true }); return h }
	defer func() { hangHook = nil }()
	s := newTestServer(1, true, 3)
	s.enqueue(&Job{WorkMs: 1})
	waitFor(t, s, 1)
	st := s.stats()
	if st["requeued_by_healthcheck"].(int64) < 1 || st["workers_total"].(int) < 2 {
		t.Fatalf("expected requeue and replacement worker: %v", st)
	}
}

func TestStaleWorkerCannotCompleteRequeuedJob(t *testing.T) {
	s := newTestServer(0, true, 3)
	j := &Job{ID: 1, State: "running", Attempts: 1, epoch: 2}
	w := &worker{}
	s.finish(w, j, 1, true) // stale epoch
	if j.State != "running" || s.done != 0 {
		t.Fatal("stale completion was accepted")
	}
}

func TestWALRecoveryRequeuesOnlyUnfinished(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.jsonl")
	s := newTestServer(0, true, 3)
	f, _ := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	s.wal, s.walSync = f, true
	a, b, c := &Job{WorkMs: 1}, &Job{WorkMs: 1}, &Job{WorkMs: 1}
	s.enqueue(a)
	s.enqueue(b)
	s.enqueue(c)
	s.logWAL(walRec{Op: "done", ID: a.ID}, false)
	s.logWAL(walRec{Op: "dead", ID: b.ID}, false)
	f.WriteString(`{"op":"sub","job":{"id":9,"wo`) // torn last line from a crash
	f.Close()

	r := &Server{jobs: map[int64]*Job{}}
	r.cond = sync.NewCond(&r.mu)
	n, err := r.recoverWAL(path)
	if err != nil || n != 1 {
		t.Fatalf("recovered %d err=%v, want 1 (job c only)", n, err)
	}
	if r.nextID != c.ID {
		t.Fatalf("nextID=%d want %d", r.nextID, c.ID)
	}
}

func TestCPUBoundJobsDoNotTriggerFalseHangDetection(t *testing.T) {
	// Long CPU jobs heartbeat from inside the loop, so a busy worker is not declared hung.
	s := newTestServer(2, true, 3)
	for i := 0; i < 6; i++ {
		s.enqueue(&Job{CPUIters: 300000})
	}
	waitFor(t, s, 6)
	if st := s.stats(); st["requeued_by_healthcheck"].(int64) != 0 || st["workers_total"].(int) != 2 {
		t.Fatalf("false positive hang detection: %v", st)
	}
}
