package config

import "strings"

//Stream represents log stream
type Stream struct {
	//Rotation represents optional log stream rotation
	Rotation     *Rotation
	FlushMod     int    //flush module to be set only for local testing
	URL          string //destination URL
	Codec        string //compression codec
	StreamUpload bool //streams controls progressive upload to s3, g3 (skip checkup)
}

//IsGzip returns true if gzip codec specified
func (s *Stream) IsGzip() bool {
	return strings.ToLower(s.Codec) == "gzip"
}

//Init initialises log stream
func (s *Stream) Init() {
	if s.Rotation != nil {
		s.Rotation.Init()
	}
	if s.IsGzip() && !strings.HasSuffix(s.URL, ".gz") {
		s.URL += ".gz"
	}
}
