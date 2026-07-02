package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type waiter struct {
	ch   chan string
	done bool
}

type queue struct {
	msgs    []string
	waiters []*waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) getQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}

	return q
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getQueue(name)

	for len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]

		if !w.done {
			w.done = true
			w.ch <- msg
			return
		}
	}

	q.msgs = append(q.msgs, msg)
}

func (b *broker) get(ctx context.Context, name string, timeout time.Duration, hasTimeout bool) (string, bool) {
	b.mu.Lock()

	q := b.getQueue(name)

	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		b.mu.Unlock()

		return msg, true
	}

	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)

	b.mu.Unlock()

	if !hasTimeout {
		select {
		case msg := <-w.ch:
			return msg, true

		case <-ctx.Done():
			b.mu.Lock()
			if !w.done {
				w.done = true
				b.removeWaiter(q, w)
			}
			b.mu.Unlock()

			return "", false
		}
	}

	if timeout <= 0 {
		b.mu.Lock()
		if !w.done {
			w.done = true
			b.removeWaiter(q, w)
		}
		b.mu.Unlock()

		return "", false
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true

	case <-timer.C:
		b.mu.Lock()
		if !w.done {
			w.done = true
			b.removeWaiter(q, w)
			b.mu.Unlock()

			return "", false
		}
		b.mu.Unlock()

		return <-w.ch, true

	case <-ctx.Done():
		b.mu.Lock()
		if !w.done {
			w.done = true
			b.removeWaiter(q, w)
		}
		b.mu.Unlock()

		return "", false
	}
}

func (b *broker) removeWaiter(q *queue, target *waiter) {
	for i, w := range q.waiters {
		if w == target {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(r.URL.Path, "/")
	if name == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		v, ok := r.URL.Query()["v"]
		if !ok || len(v) == 0 || v[0] == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b.put(name, v[0])
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		timeout, hasTimeout, err := parseTimeout(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		msg, ok := b.get(r.Context(), name, timeout, hasTimeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_, _ = w.Write([]byte(msg))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func parseTimeout(r *http.Request) (time.Duration, bool, error) {
	raw := r.URL.Query().Get("timeout")
	if raw == "" {
		return 5 * time.Second, true, nil
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		return 0, false, fmt.Errorf("invalid timeout")
	}

	return time.Duration(seconds) * time.Second, true, nil
}

func main() {
	port := flag.String("port", "8080", "server port")
	flag.Parse()

	if err := http.ListenAndServe(":"+*port, newBroker()); err != nil {
		panic(err)
	}
}
