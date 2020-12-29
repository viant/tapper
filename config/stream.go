package config

import (
	"strings"
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
	format *Format
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
