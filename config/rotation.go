package config

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)




//Rotation rotation rotation config
type Rotation struct {
	EveryMs        int
	MaxEntries     int
	Format
	URL string
	Codec          string
	Emit           *Event
	url            string
	hasSeq         bool

	sequence       int32
}

//IsGzip returns true if gzip codec specified
func (r *Rotation) IsGzip() bool {
	return strings.ToLower(r.Codec) == "gzip"
}

//Init initialises rotation
func (r *Rotation) Init() {
	r.Format.Init(r.URL)
	r.hasSeq = strings.Contains(r.URL, "%")
	if r.Emit != nil {
		r.Emit.Init()
	}
}

//ExpiryTime returns expiry time
func (r Rotation) ExpiryTime(created time.Time) *time.Time {
	if r.EveryMs == 0 {
		return nil
	}
	expiry := created.Add(time.Duration(r.EveryMs) * time.Millisecond)
	return &expiry
}

//ExpandURL expand rotation Format with log sequence,  time and ID
func (r *Rotation) ExpandURL(t time.Time, ID string) string {
	URL := r.Format.ExpandURL(t, r.URL)
	if !r.hasSeq {
		return URL
	}
	if r.url == URL {
		atomic.AddInt32(&r.sequence, 1)
	} else {
		atomic.StoreInt32(&r.sequence, 0)
	}
	r.url = URL
	return fmt.Sprintf(URL, fmt.Sprintf("%v-%v", ID, atomic.LoadInt32(&r.sequence)))
}
