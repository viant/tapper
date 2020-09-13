package config

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
	"sync/atomic"
	"time"
)

//Rotation rotation rotation config
type Rotation struct {
	EveryMs        int
	MaxEntries     int
	URL            string
	Codec          string
	Emit           *Event
	url            string
	hasSeq         bool
	timeStartIndex int
	timeEndIndex   int
	timeLayout     string
	sequence       int32
}

//IsGzip returns true if gzip codec specified
func (r *Rotation) IsGzip() bool {
	return strings.ToLower(r.Codec) == "gzip"
}

//Init initialises rotation
func (r *Rotation) Init() {
	r.hasSeq = strings.Contains(r.URL, "%")
	r.timeLayout = r.URL
	r.timeEndIndex = strings.Index(r.URL, "]")
	r.timeStartIndex = strings.Index(r.URL, "[")
	if r.timeStartIndex != -1 && r.timeEndIndex == -1 {
		timeTemplate := r.URL[r.timeStartIndex+1 : r.timeEndIndex]
		r.timeLayout = toolbox.DateFormatToLayout(timeTemplate)
	}
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

//ExpandURL expand rotation URL with log sequence,  time and ID
func (r *Rotation) ExpandURL(t time.Time, ID string) string {
	URL := r.URL
	if r.timeEndIndex > 0 {
		timeValue := t.Format(r.timeLayout)
		URL = r.URL[:r.timeStartIndex] + timeValue + r.URL[r.timeEndIndex+1:]
	}
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
