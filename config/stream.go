package config

import (
	"math/rand"
	"strings"
	"sync"
	"time"
)

//Stream represents log stream
type Stream struct {
	//Rotation represents optional log stream rotation
	Rotation     *Rotation
	FlushMod     int    //flush module to be set only for local testing
	URL          string //destination URL
	timeLayout   string
	Codec        string //compression codec
	StreamUpload bool   //streams controls progressive upload to s3, g3 (skip checkup)
	format       *Format
	SamplePct    *float64 //sample pct (0..100)
	mux          sync.Mutex
	sampler      *rand.Rand
}

//CanSample returns true if sample pct is not configured or sample random value meets target
func (s *Stream) CanSample() bool {
	if s.SamplePct == nil || *s.SamplePct == 100 {
		return true
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.sampler == nil {
		s.sampler = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	target := s.sampler.Float64()
	return (target * 100) < *s.SamplePct
}

//IsGzip returns true if gzip codec specified
func (s *Stream) IsGzip() bool {
	return strings.ToLower(s.Codec) == "gzip"
}

//Init initialises log stream
func (s *Stream) Init() {
	if s.Rotation != nil {
		s.Rotation.Init()
	} else if s.format == nil {
		s.format = &Format{}
		s.format.Init(s.URL)
		s.URL = s.format.ExpandURL(time.Now(), s.URL)
	}
	if s.IsGzip() && !strings.HasSuffix(s.URL, ".gz") {
		s.URL += ".gz"
	}
}
