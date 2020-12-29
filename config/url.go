package config

import (
	"github.com/viant/toolbox"
	"strings"
	"time"
)

type Format struct {
	timeStartIndex int
	timeEndIndex   int
	timeLayout     string
}

func (r *Format) Init(URL string) {
	r.timeLayout = URL
	r.timeEndIndex = strings.Index(URL, "]")
	r.timeStartIndex = strings.Index(URL, "[")
	if r.timeStartIndex != -1 && r.timeEndIndex == -1 {
		timeTemplate := URL[r.timeStartIndex+1 : r.timeEndIndex]
		r.timeLayout = toolbox.DateFormatToLayout(timeTemplate)
	}
}

func (r *Format) ExpandURL(t time.Time, URL string) string {
	if r.timeEndIndex > 0 {
		timeValue := t.Format(r.timeLayout)
		return URL[:r.timeStartIndex] + timeValue + URL[r.timeEndIndex+1:]
	}
	return URL
}