package consumer

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/tapper/emitter/consumer/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v2"
)

//Config represents streamer config
type Config struct {
	Port    string
	Streams []*config.Command
}

//Validate checks if config is valid
func (c Config) Validate() error {
	if c.Port == "" {
		return errors.New("port was empty")
	}
	if len(c.Streams) == 0 {
		return errors.Errorf("Streams were empty")
	}
	return nil
}

//NewConfigFromURL creates a config from URL
func NewConfigFromURL(URL string) (*Config, error) {
	fs := afs.New()
	reader, err := fs.OpenURL(context.Background(), URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open config URL: %v", URL)
	}
	defer reader.Close()
	YAML := map[string]interface{}{}
	err = yaml.NewDecoder(reader).Decode(YAML)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode YAML")
	}
	cfg := &Config{}
	return cfg, toolbox.DefaultConverter.AssignConverted(cfg, YAML)
}
