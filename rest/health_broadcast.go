package rest

import (
	"sync"
	"time"
)

type Monitor struct {
	Health   bool
	Critical bool
}

type HealthBroadcast struct {
	mu       sync.Mutex
	monitor  Monitor
	subs     []chan Monitor
	lastDown *time.Time
	downTime int64
}

func NewHealthBroadcast() *HealthBroadcast {
	return &HealthBroadcast{
		monitor: Monitor{true, false},
	}
}

func (hm *HealthBroadcast) Subscribe() <-chan Monitor {
	ch := make(chan Monitor, 1)
	hm.mu.Lock()
	hm.subs = append(hm.subs, ch)
	ch <- hm.monitor
	hm.mu.Unlock()
	return ch
}

func (hm *HealthBroadcast) SetHealth(health bool) {
	hm.mu.Lock()
	downTime := hm.downTime
	if !health {
		if hm.lastDown == nil {
			t := time.Now().UTC()
			hm.lastDown = &t
		}
		downTime += time.Since(*hm.lastDown).Milliseconds()
	} else if hm.lastDown != nil {
		downTime += time.Since(*hm.lastDown).Milliseconds()
		hm.downTime = downTime
		hm.lastDown = nil
	}

	isCritical := downTime > 39000

	if hm.monitor.Health == health && isCritical == hm.monitor.Critical {
		hm.mu.Unlock()
		return
	}

	hm.monitor.Health = health
	hm.monitor.Critical = isCritical
	for _, ch := range hm.subs {
		select {
		case ch <- hm.monitor:
		default:
		}
	}
	hm.mu.Unlock()
}
