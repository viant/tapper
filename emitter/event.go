package emitter

import (
	"github.com/viant/tapper/config"
	"math/rand"
	"time"
)

//Event represents an event
type Event struct {
	Config  *config.Event
	Created time.Time
	URL     string
	attempt int
	nextRun *time.Time
}

//SetNextRun set next run
func (e *Event) SetNextRun(now time.Time) {
	if e.attempt == 0 {
		e.attempt = 1
	}
	randDelayMs := rand.Int31n(int32(e.attempt * 10))
	nextRun := now.Add(time.Second * time.Duration(randDelayMs))
	e.nextRun = &nextRun
}
