package rest

import (
	"sync"
	"time"
)

type HealthMonitor struct {
	health   bool
	mu       *sync.Mutex
	cond     *sync.Cond
	lastDown *time.Time
	downTime int64
}

func (hm *HealthMonitor) SetHealth(health bool) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.health = health
	if health {
		if hm.lastDown != nil {
			hm.downTime = +time.Since(*hm.lastDown).Milliseconds()
			hm.lastDown = nil
			hm.cond.Broadcast()
		}
	} else {
		if hm.lastDown == nil {
			now := time.Now().UTC()
			hm.lastDown = &now
			return
		}
		if hm.getDownTime() > 20000 {
			hm.cond.Broadcast()
		}
	}
}

func (hm *HealthMonitor) GetHealth() bool {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	return hm.health
}

func (hm *HealthMonitor) IsCritical() bool {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	return hm.getDownTime() > 20000
}

func (hm *HealthMonitor) AwaitHealth() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	for !hm.health {
		hm.cond.Wait()
	}
}

func (hm *HealthMonitor) getDownTime() int64 {
	var downTime int64
	if hm.lastDown != nil {
		downTime = time.Since(*hm.lastDown).Milliseconds()
	}
	return downTime + hm.downTime
}

func NewHealthMonitor() *HealthMonitor {
	mutex := sync.Mutex{}
	return &HealthMonitor{true, &mutex, sync.NewCond(&mutex), nil, 0}
}
