package log_test

import (
	"github.com/viant/afs"
	"github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
	"github.com/viant/tapper/msg"
	slog "log"
)

func ExampleLogger_Log() {

	cfg := &config.Stream{
		URL: "/tmp/logfile.log",
		Rotation: &config.Rotation{
			EveryMs: 100,
			URL:     "/tmp/logfile.log.[yyyyMMdd_HH]-%v",
		},
	}
	logger, err := log.New(cfg, "myID", afs.New())
	if err != nil {
		slog.Fatal(err)
	}
	provider := msg.NewProvider(2048, 32)

	for i := 0; i < 100; i++ {
		message := provider.NewMessage()
		message.PutString("k1", "value1")
		message.PutInt("k2", 2)
		message.PutStrings("k3", []string{"1", "3"})
		err = logger.Log(message)
		if err != nil {
			slog.Fatal(err)
		}
		message.Free()
	}
	logger.Close()
}
