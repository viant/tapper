package log

import (
	"github.com/viant/afs"
	"github.com/viant/tapper/config"
	"github.com/viant/tapper/emitter"
	"github.com/viant/tapper/msg"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Logger represents file system transaction logger
type Logger struct {
	ID      string
	fs      afs.Service
	count   int32
	mux     *sync.Mutex
	config  *config.Stream
	index   uint64
	writers []*writer
	closed  int32
	emitter *emitter.Service
}

func (l *Logger) monitorWriters() {
	for atomic.LoadInt32(&l.closed) == 0 {
		l.mux.Lock()
		writer := l.getWriter()
		if writer == nil {
			continue
		}
		if writer.isExpired(time.Now().Add(-5 * time.Second)) {
			if err := writer.Flush(); err == nil {
				err = l.rotateIfNeeded(writer, time.Now())
			}
		}
		l.mux.Unlock()
		time.Sleep(time.Second)
	}
}

//Close closes logger
func (l *Logger) Close() (err error) {
	atomic.StoreInt32(&l.closed, 1)
	for _, writer := range l.writers {
		if writer == nil {
			continue
		}
		writer.loggerClose = true
		if e := writer.Close(); e != nil {
			err = e
		}
	}
	l.emitter.Close()
	return err
}

func (l *Logger) open(ts time.Time) (err error) {
	var rotationURL string
	if l.config.Rotation != nil {
		rotationURL = l.config.Rotation.ExpandURL(ts, l.ID)
	}

	index := atomic.AddUint64(&l.index, 1) % 2
	l.writers[index], err = newWriter(l.config, l.fs, rotationURL, int(index), ts, l.emitter)
	return err
}

func (l *Logger) getWriter() *writer {
	index := atomic.LoadUint64(&l.index) % 2
	return l.writers[index]
}

//Log logs a message
func (l *Logger) Log(message *msg.Message) (err error) {
	now := time.Now()
	l.mux.Lock()
	defer l.mux.Unlock()
	writer := l.getWriter()
	_, err = message.WriteTo(writer)
	if err == nil {
		count := writer.increment()
		if l.config.FlushMod > 0 && count&l.config.FlushMod == 0 {
			err = writer.Flush()
		}
		if err == nil {
			err = l.rotateIfNeeded(writer, now)
		}
	}
	return err
}

func (l *Logger) rotateIfNeeded(writer *writer, now time.Time) (err error) {
	if writer.isMaxReached() || writer.isExpired(now) {
		if err = writer.Close(); err == nil {
			l.writers[writer.index] = nil
			err = l.open(now)
		}
	}
	return err
}

//New creates a transaction logger
func New(config *config.Stream, ID string, fs afs.Service) (*Logger, error) {
	config.Init()
	emitter, err := emitter.New(config)
	if err != nil {
		return nil, err
	}
	result := &Logger{
		fs:      fs,
		mux:     &sync.Mutex{},
		config:  config,
		ID:      strings.Replace(ID, ".", "_", len(ID)),
		writers: make([]*writer, 2),
		emitter: emitter,
	}
	err = result.open(time.Now())
	if err != nil {
		return nil, err
	}
	if config.Rotation != nil {
		go result.monitorWriters()
	}
	return result, err
}
