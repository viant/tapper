package log_test

import (
	"compress/gzip"
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
	"github.com/viant/tapper/msg"
	"github.com/viant/toolbox"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogger_Log(t *testing.T) {

	var useCases = []struct {
		description         string
		rotation            string
		config              *config.Stream
		poolSize            int
		bufferSize          int
		messages            int
		emitterConsumerPort string
		expectEmitterParams map[string]string
	}{
		{
			description: "temp logging",
			config: &config.Stream{
				FlushMod: 10000,
				URL:      "/tmp/tapper-test.json",
			},
			poolSize:   2,
			bufferSize: 128,
			messages:   1000,
		},
		{
			description: "rotation test",
			config: &config.Stream{
				Rotation: &config.Rotation{
					EveryMs:    0,
					MaxEntries: 1000,
					URL:        "/tmp/tapper-rtest-%v",
				},
				FlushMod: 100,
				URL:      "/tmp/tapper-rtest",
			},
			rotation:   "/tmp/tapper-rtest-127_0_0_1-0",
			poolSize:   2,
			bufferSize: 128,
			messages:   1000,
		},
		{
			description: "rotation test compression",
			config: &config.Stream{
				Rotation: &config.Rotation{
					EveryMs:    0,
					MaxEntries: 1000,
					URL:        "/tmp/tapper-rtest-%v",
					Codec:      "gzip",
				},
				FlushMod: 100,
				URL:      "/tmp/tapper-rtest",
			},
			rotation:   "/tmp/tapper-rtest-127_0_0_1-0.gz",
			poolSize:   2,
			bufferSize: 128,
			messages:   1000,
		},

		{
			description:         "rotation event test",
			emitterConsumerPort: "8199",
			config: &config.Stream{
				Rotation: &config.Rotation{
					EveryMs:    0,
					MaxEntries: 1000,
					URL:        "/tmp/emitter/tapper-rtest-%v",
					Emit: &config.Event{
						URL: "http://127.0.0.1:8199",
						Params: map[string]string{
							"Path": "$DestPath",
							"URL":  "$Dest",
							"Name": "$DestName",
							"Time": "$TimePath",
						},
					},
				},
				FlushMod: 100,
				URL:      "/tmp/emitter/tapper-rtest",
			},
			rotation:   "/tmp/emitter/tapper-rtest-127_0_0_1-0",
			poolSize:   2,
			bufferSize: 128,
			messages:   1000,
			expectEmitterParams: map[string]string{
				"Name": "tapper-rtest-127_0_0_1-0",
				"Path": "/tmp/emitter/tapper-rtest-127_0_0_1-0",
				"URL":  "/tmp/emitter/tapper-rtest-127_0_0_1-0",
			},
		},

		{
			description: "compressed rotation event test",
			config: &config.Stream{
				Rotation: &config.Rotation{
					EveryMs:    0,
					MaxEntries: 1000,
					URL:        "/tmp/cemitter/tapper-rtest-%v",
					Codec:      "gzip",
					Emit: &config.Event{
						URL: "http://127.0.0.1:8198",
						Params: map[string]string{
							"Path": "$DestPath",
							"URL":  "$Dest",
							"Name": "$DestName",
							"Time": "$TimePath",
						},
					},
				},
				FlushMod: 100,
				URL:      "/tmp/cemitter/tapper-rtest",
			},
			emitterConsumerPort: "8198",
			rotation:            "/tmp/cemitter/tapper-rtest-127_0_0_1-0.gz",
			poolSize:            2,
			bufferSize:          128,
			messages:            1000,
			expectEmitterParams: map[string]string{
				"Name": "tapper-rtest-127_0_0_1-0.gz",
				"Path": "/tmp/cemitter/tapper-rtest-127_0_0_1-0.gz",
				"URL":  "/tmp/cemitter/tapper-rtest-127_0_0_1-0.gz",
			},
		},
	}
	fs := afs.New()
	ctx := context.Background()
	for _, useCae := range useCases {

		if useCae.rotation != "" {
			parent, _ := url.Split(useCae.rotation, file.Scheme)
			fs.Create(ctx, parent, file.DefaultDirOsMode, true)
		}
		if ok, _ := fs.Exists(ctx, useCae.config.URL); ok {
			fs.Delete(ctx, useCae.config.URL)
		}
		if useCae.rotation != "" {
			fs.Delete(ctx, useCae.rotation)
		}
		var srv *testServer
		if useCae.emitterConsumerPort != "" {
			srv = newTestServer(useCae.emitterConsumerPort)
			go srv.ListenAndServe()
			time.Sleep(100 * time.Millisecond)
		}

		provider := msg.NewProvider(useCae.bufferSize, useCae.poolSize)
		logger, err := log.New(useCae.config, "127.0.0.1", fs)
		if !assert.Nil(t, err) {
			return
		}

		for i := 0; i < useCae.messages; i++ {
			message := provider.NewMessage()
			message.PutInt("id", i)
			message.PutString("k1", strings.Repeat("?", 50))
			message.PutFloat("k2", float64(i)*100.0)
			message.PutBool("k3", true)
			err = logger.Log(message)
			assert.Nil(t, err)
			message.Free()
		}
		err = logger.Close()
		if !assert.Nil(t, err, useCae.description) {
			continue
		}

		location := useCae.config.URL
		if useCae.rotation != "" {
			time.Sleep(100 * time.Millisecond)
			location = useCae.rotation
		}

		reader, err := fs.OpenURL(ctx, location)
		if strings.HasSuffix(location, ".gz") {
			reader, _ = gzip.NewReader(reader)
		}
		if !assert.Nil(t, err, useCae.description) {
			continue
		}
		data, err := ioutil.ReadAll(reader)
		if !assert.Nil(t, err, useCae.description) {
			continue
		}

		lines := strings.Split(string(data), "\n")
		if srv != nil {
			time.Sleep(100 * time.Millisecond)
			if !assert.True(t, len(srv.Params) > 0) {

			}
			params := srv.Params[len(srv.Params)-1]
			for k, v := range useCae.expectEmitterParams {
				assert.EqualValues(t, v, params[k], useCae.description+" / "+k)
			}
		}
		assert.Equal(t, len(lines)-1, useCae.messages)
		_ = reader.Close()

	}
}

//Server represents consumer server
type testServer struct {
	*http.Server
	sync.Mutex
	Params []map[string]string
}

func (s *testServer) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	httpRequest.ParseForm()
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	aMap := map[string]string{}
	if len(httpRequest.Form) > 0 {
		for key := range httpRequest.Form {
			aMap[key] = httpRequest.Form.Get(key)
		}
	}
	s.Params = append(s.Params, aMap)
	writer.WriteHeader(http.StatusOK)
}

func newTestServer(port string) *testServer {
	result := &testServer{
		Params: make([]map[string]string, 0),
	}
	mux := http.NewServeMux()
	mux.Handle("/", result)
	result.Server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}
	return result
}

func BenchmarkLogger_Log(b *testing.B) {
	toolbox.RemoveFileIfExist("/tmp/tapper_bench.log")
	cfg := &config.Stream{
		URL: "/tmp/tapper_bench.log",
	}
	testConcurrently(b, cfg)
}

func BenchmarkLogger_Log_Rotation(b *testing.B) {
	toolbox.RemoveFileIfExist("/tmp/tapper_bench_rotation.log")
	cfg := &config.Stream{
		URL: "/tmp/tapper_bench_rotation.log",
		Rotation: &config.Rotation{
			EveryMs:    0,
			MaxEntries: 1000,
			URL:        "/tmp/tapper_bench_rotation-%v.log",
		},
	}
	testConcurrently(b, cfg)
}

func testConcurrently(b *testing.B, cfg *config.Stream) {
	messages := msg.NewProvider(2048, 1024)
	logger, err := log.New(cfg, "xx", afs.New())
	if !assert.Nil(b, err) {
		b.Log(err)
	}
	b.ResetTimer()
	data := strings.Repeat("?", 128)
	b.RunParallel(func(pb *testing.PB) {
		b.ReportAllocs()
		for pb.Next() {
			message := messages.NewMessage()
			message.PutString("att1", "1")
			message.PutString("data1", data)
			message.PutString("data2", data)
			message.PutString("data3", data)
			message.PutString("data4", data)
			message.PutString("data5", data)
			message.PutString("data6", data)
			message.PutString("data7", data)
			message.PutString("data8", data)
			message.PutString("data9", data)
			message.PutString("data10", data)
			err = logger.Log(message)
			assert.Nil(b, err)
			message.Free()
		}
	})
	logger.Close()
}
