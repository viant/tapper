package consumer

import (
	"github.com/pkg/errors"
	"github.com/viant/afs/url"
	"github.com/viant/tapper/emitter/consumer/config"
	"log"
	"os/exec"
	"strings"
)

//Service represents simple rotation event consumer to handle rotated logs.
type Service struct {
	registry map[string]*config.Command
}

//Consume consumes rotation event
func (s *Service) Consume(request *Request) error {
	URLPath := url.Path(request.URL)
	if index := strings.Index(URLPath, "?"); index != -1 {
		URLPath = URLPath[:index]
	}
	command, ok := s.registry[URLPath]
	if !ok {
		return errors.Errorf("failed to lookup command for: %v", URLPath)
	}
	args := command.ExpandArgs(request.Params)
	cmd := exec.Command(command.Name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		output = []byte(err.Error())
	}
	log.Printf("%v %v\n%s", command.Name, strings.Join(args, " "), output)
	if err != nil {
		return errors.Wrapf(err, "failed to start command: %v[%v]", command.Name, strings.Join(args, " "))
	}

	return nil
}

//New creates a new rotation consumer service
func New(cfg *Config) (*Service, error) {
	srv := &Service{registry: make(map[string]*config.Command)}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	for i, stream := range cfg.Streams {
		srv.registry[stream.URI] = cfg.Streams[i]
	}
	return srv, nil
}
