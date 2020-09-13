package emitter

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/tapper/config"
	"io/ioutil"
	"net/http"
	u "net/url"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	scheduleLoopSleepTime = 100 * time.Millisecond
)

//Service represents emitter service
type Service struct {
	_pending map[string]*Event
	mux      sync.Mutex
	closed   int32
	fs       afs.Service
}

//Close closes service
func (s *Service) Close() error {
	atomic.StoreInt32(&s.closed, 1)
	return nil
}

//Schedule schedules an event
func (s *Service) Schedule(event *Event) error {
	if event.attempt > event.Config.MaxRetries && event.Config.MaxRetries > 0 {
		return errors.Errorf("max retries reached: %v", event.Config.MaxRetries)
	}
	event.SetNextRun(time.Now())
	event.attempt++
	s.mux.Lock()
	s._pending[event.URL] = event
	s.mux.Unlock()
	return nil
}

//Emit emits an event
func (s *Service) Emit(event *Event) error {
	err := s.emit(event)
	if err != nil {
		s.Schedule(event)
	}
	return err
}

func (s *Service) emit(event *Event) error {
	if event.Config.URL != "" {
		return s.sendNotification(event)
	}
	return s.execute(event)
}

func (s *Service) sendNotification(event *Event) error {
	values := u.Values{}
	params := expandParameters(event.Config.Params, event.URL, event.Created)
	for k, v := range params {
		values.Set(k, v)
	}
	URL := event.Config.URL + "?" + values.Encode()
	response, err := http.Get(URL)
	if err != nil {
		return errors.Wrapf(err, "failed to send request %v", err)
	}
	var message []byte
	if response.Body != nil {
		message, _ = ioutil.ReadAll(response.Body)
		_ = response.Body.Close()
	}
	if response.StatusCode != http.StatusOK {
		return errors.Errorf("invalid response: %v, %s, for %v", response.StatusCode, message, URL)
	}
	return nil
}

func (s *Service) execute(event *Event) error {
	args := expandArguments(event.Config.Args, event.URL, event.Created)
	cmd := exec.Command(event.Config.Command, args...)
	output, err := cmd.CombinedOutput()
	if len(output) > 0 {
		fmt.Printf("%s\n", output)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to start command: %v[%v]", event.Config.Command, strings.Join(args, " "))
	}

	return nil
}

func (s *Service) pending() []*Event {
	var result = make([]*Event, 0)
	s.mux.Lock()
	defer s.mux.Unlock()
	now := time.Now()
	for k, v := range s._pending {
		if v.nextRun.After(now) {
			v.attempt++
			result = append(result, s._pending[k])
		}
	}
	return result
}

func (s *Service) handleScheduled() {
	for atomic.LoadInt32(&s.closed) == 0 {
		pending := s.pending()
		if len(pending) > 0 {
			for i, item := range pending {
				s.mux.Lock()
				delete(s._pending, item.URL)
				s.mux.Unlock()
				_ = s.Emit(pending[i])
			}
		}
		time.Sleep(scheduleLoopSleepTime)
	}
}

func (s *Service) loadPending(stream *config.Stream) error {

	if stream.Rotation == nil || stream.Rotation.Emit == nil {
		return nil
	}
	parent, name := url.Split(stream.Rotation.URL, file.Scheme)
	index := strings.Index(name, "[")
	if seqIndex := strings.Index(name, "%"); seqIndex != -1 && seqIndex < index || index == -1 {
		index = seqIndex
	}
	rotationPrefix := name
	if index != -1 {
		rotationPrefix = name[:index]
	}
	objects, err := s.fs.List(context.Background(), parent)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		if strings.HasPrefix(object.Name(), rotationPrefix) {
			event := &Event{
				Config:  stream.Rotation.Emit,
				Created: object.ModTime(),
				URL:     object.URL(),
			}
			_ = s.Emit(event)
		}
	}
	return nil
}

//New creates new service
func New(stream *config.Stream) (*Service, error) {
	result := &Service{_pending: make(map[string]*Event), fs: afs.New()}
	go func() {
		result.loadPending(stream)
		result.handleScheduled()
	}()
	return result, nil
}
