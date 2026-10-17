// loadgen: submits N jobs over HTTP with C concurrent clients, waits for the
// queue to drain (or a timeout), and prints one JSON result line.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080", "orchd url")
	n := flag.Int("n", 2000, "jobs")
	c := flag.Int("c", 32, "client concurrency")
	work := flag.Int("work-ms", 20, "work per job")
	fail := flag.Float64("fail", 0, "failure prob per attempt")
	hang := flag.Float64("hang", 0, "hang prob per attempt")
	timeout := flag.Duration("timeout", 30*time.Second, "drain timeout")
	label := flag.String("label", "", "label")
	flag.Parse()
	start := time.Now()
	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ch {
				b, _ := json.Marshal(map[string]interface{}{"work_ms": *work, "fail_prob": *fail, "hang_prob": *hang})
				r, err := http.Post(*url+"/jobs", "application/json", bytes.NewReader(b))
				if err == nil {
					r.Body.Close()
				}
			}
		}()
	}
	for i := 0; i < *n; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
	submitDur := time.Since(start)
	var st map[string]float64
	for time.Since(start) < *timeout {
		r, err := http.Get(*url + "/stats")
		if err == nil {
			json.NewDecoder(r.Body).Decode(&st)
			r.Body.Close()
			if st["done"]+st["dead"] >= float64(*n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	total := time.Since(start)
	out := map[string]interface{}{"label": *label, "jobs": *n, "done": st["done"], "dead": st["dead"],
		"completion_pct": 100 * st["done"] / float64(*n), "retried": st["retried"], "requeued": st["requeued_by_healthcheck"],
		"submit_s": submitDur.Seconds(), "total_s": total.Seconds(), "throughput_jobs_s": st["done"] / total.Seconds()}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
