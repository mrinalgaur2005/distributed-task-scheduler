package queue

import (
	"time"
)

type CircuitBreaker struct {
	failCount int
	state     string
	lastFail  time.Time
}

func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{state: "CLOSED"}
}

func (cb *CircuitBreaker) Ready() bool {
	if cb.state == "OPEN" && time.Since(cb.lastFail) < 10*time.Second {
		return false
	}
	if cb.state == "OPEN" && time.Since(cb.lastFail) >= 10*time.Second {
		cb.Reset()
	}
	return true
}

func (cb *CircuitBreaker) Fail() {
	cb.failCount++
	cb.lastFail = time.Now()
	if cb.failCount >= 3 {
		cb.state = "OPEN"
	}
}

func (cb *CircuitBreaker) Success() {
	cb.failCount = 0
	cb.state = "CLOSED"
}

func (cb *CircuitBreaker) Reset() {
	cb.failCount = 0
	cb.state = "CLOSED"
}
