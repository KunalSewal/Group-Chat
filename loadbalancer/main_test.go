package main

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"
)

// A ResponseWriter that gives up the way the real one does when the client has
// gone away mid-response: httputil.ReverseProxy turns a failed copy into a
// panic with http.ErrAbortHandler, which net/http recovers at the connection
// level — above anything in this file. Any bookkeeping not unwound by a defer
// is lost at that moment, and under load that moment arrives constantly.
type abortingWriter struct{ http.ResponseWriter }

func (a *abortingWriter) Write(p []byte) (int, error) {
	panic(http.ErrAbortHandler)
}

func TestInFlightReturnsToZeroAfterAnAbortedResponse(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("a response the client will not be around to read"))
	}))
	defer origin.Close()

	target, err := url.Parse(origin.URL)
	if err != nil {
		t.Fatal(err)
	}

	backend := &Backend{URL: target, proxy: httputil.NewSingleHostReverseProxy(target)}
	backend.Alive.Store(true)

	lb := &LoadBalancer{
		metrics:  newMetrics(256),
		weights:  Weights{InFlight: 1, Latency: 0.05, CPU: 0.10},
		backends: []*Backend{backend},
	}
	lb.SetThreshold(4)

	const aborted = 25
	for i := 0; i < aborted; i++ {
		func() {
			defer func() {
				if recovered := recover(); recovered != http.ErrAbortHandler {
					t.Fatalf("expected ErrAbortHandler, got %v", recovered)
				}
			}()

			lb.ServeHTTP(&abortingWriter{httptest.NewRecorder()},
				httptest.NewRequest(http.MethodGet, "/feed", nil))
		}()
	}

	if got := backend.InFlight.Load(); got != 0 {
		t.Fatalf("in-flight leaked %d of %d aborted requests; the balancer would route "+
			"on a score permanently wrong by that much", got, aborted)
	}
}

// A tunnelled connection is held open by whoever opened it, not by the backend
// being busy, so it must not weigh on the routing decision.
func TestUpgradedConnectionsAreNotCountedAsLoad(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer origin.Close()

	target, _ := url.Parse(origin.URL)
	backend := &Backend{URL: target, proxy: httputil.NewSingleHostReverseProxy(target)}
	backend.Alive.Store(true)

	lb := &LoadBalancer{
		metrics:  newMetrics(256),
		weights:  Weights{InFlight: 1, Latency: 0.05, CPU: 0.10},
		backends: []*Backend{backend},
	}
	lb.SetThreshold(4)

	r := httptest.NewRequest(http.MethodGet, "/socket.io/", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	lb.ServeHTTP(httptest.NewRecorder(), r)

	if got := backend.InFlight.Load(); got != 0 {
		t.Fatalf("an upgraded connection was counted as queued work: in_flight = %d", got)
	}
}

func TestUpgradeDetection(t *testing.T) {
	cases := []struct {
		name       string
		connection []string
		upgrade    string
		want       bool
	}{
		{"plain request", nil, "", false},
		{"websocket", []string{"Upgrade"}, "websocket", true},
		{"lowercase", []string{"upgrade"}, "websocket", true},
		{"token in a list", []string{"keep-alive, Upgrade"}, "websocket", true},
		{"separate headers", []string{"keep-alive", "Upgrade"}, "websocket", true},
		// Must not match a value that merely contains the word.
		{"not a token", []string{"upgrade-insecure-requests"}, "websocket", false},
		{"connection without upgrade header", []string{"Upgrade"}, "", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/socket.io/", nil)
			for _, value := range c.connection {
				r.Header.Add("Connection", value)
			}
			if c.upgrade != "" {
				r.Header.Set("Upgrade", c.upgrade)
			}
			if got := isUpgrade(r); got != c.want {
				t.Fatalf("isUpgrade = %v, want %v", got, c.want)
			}
		})
	}
}

// A backend that accepts the request, sends headers, and then simply stops is
// the case -backend-timeout does not cover: the headers arrived, so nothing is
// waiting on them any more, and the body never comes. Without an overall
// deadline the goroutine and its connection are held for the life of the
// process, and the in-flight count the routing decision uses never comes back
// down.
func TestAStalledResponseDoesNotHoldTheRequestForever(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer origin.Close()

	target, _ := url.Parse(origin.URL)
	backend := &Backend{URL: target, proxy: httputil.NewSingleHostReverseProxy(target)}
	backend.Alive.Store(true)

	lb := &LoadBalancer{
		metrics:  newMetrics(256),
		weights:  Weights{InFlight: 1, Latency: 0.05, CPU: 0.10},
		backends: []*Backend{backend},
	}
	lb.SetThreshold(4)

	previous := requestDeadline
	requestDeadline = 300 * time.Millisecond
	defer func() { requestDeadline = previous }()

	done := make(chan struct{})
	go func() {
		defer func() {
			recover() // an aborted copy surfaces as ErrAbortHandler
			close(done)
		}()
		lb.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/feed", nil))
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled response held the request past its deadline")
	}

	if got := backend.InFlight.Load(); got != 0 {
		t.Fatalf("in-flight stuck at %d after the deadline expired", got)
	}
}
