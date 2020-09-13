package log

import (
	"bufio"
	"compress/gzip"
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/tapper/config"
	"github.com/viant/tapper/emitter"
	iow "github.com/viant/tapper/io"
	"io"
	"log"
	"os"
	"path"
	"sync/atomic"
	"time"
)

//writer represents an optimized writer
type writer struct {
	index            int
	emitter          *emitter.Service
	destURL          string
	rotationURL      string
	rotationPath     string
	rotationTransfer bool
	codec            string
	count            int32
	max              int32
	flushMod         int
	closed           int32
	created          time.Time
	expiryTime       *time.Time
	closer           io.Closer
	flusher          iow.Flusher
	writer           io.Writer
	config           *config.Stream
	fs               afs.Service
}

//Close closes this writer
func (w *writer) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	if w.rotationURL == "" {
		return nil
	}
	if w.rotationPath != "" {
		src := url.Path(w.destURL)
		if err := os.Rename(src, w.rotationPath); err != nil {
			return errors.Wrapf(err, "failed to rename: %v to %v", src, w.rotationPath)
		}
	}
	go func() {
		if err := w.closeQuietly(); err != nil {
			log.Print(err)
		}
	}()
	return nil
}

func (w *writer) closeQuietly() error {
	ctx := context.Background()
	err := w.Flush()
	if writerCloser, ok := w.writer.(io.Closer); ok && err == nil {
		err = writerCloser.Close()
	}
	if err == nil {
		if err = w.closer.Close(); err == nil {
			if w.config.Rotation.IsGzip() {
				if err = w.compress(ctx); err != nil {
					return err
				}
			}
		}
	}
	if err == nil {
		if err = w.transferToDestURL(ctx); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	if w.emitter != nil && w.count > 0 {
		event := &emitter.Event{
			Config:  w.config.Rotation.Emit,
			Created: w.created,
			URL:     w.rotationURL,
		}
		err = w.emitter.Emit(event)
	}
	return err
}

func (w *writer) transferToDestURL(ctx context.Context) error {
	if !w.rotationTransfer {
		return nil
	}
	rotation := w.config.Rotation
	if rotation == nil {
		return nil
	}
	if rotation.IsGzip() {
		return nil
	}
	reader, err := w.fs.OpenURL(ctx, w.rotationPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	writer, err := w.fs.NewWriter(ctx, w.rotationURL, file.DefaultFileOsMode)
	if err != nil {
		return err
	}
	if _, err = io.Copy(writer, reader); err != nil {
		return err
	}
	return writer.Close()
}

func (w *writer) compress(ctx context.Context) (err error) {
	if w.rotationURL == "" {
		return
	}
	if !w.config.Rotation.IsGzip() {
		return
	}
	source := w.rotationURL
	if w.rotationPath != "" {
		source = w.rotationPath
	}
	reader, err := w.fs.OpenURL(ctx, source)
	if err != nil {
		return err
	}
	defer reader.Close()
	w.rotationURL += ".gz"
	destWriter, err := w.fs.NewWriter(ctx, w.rotationURL, file.DefaultFileOsMode)
	if err != nil {
		return err
	}
	defer destWriter.Close()
	writer := gzip.NewWriter(destWriter)
	if _, err = io.Copy(writer, reader); err == nil {
		if err = writer.Flush(); err == nil {
			if err = writer.Close(); err == nil {
				err = w.fs.Delete(ctx, source)
			}
		}
	}
	return err
}

//isMaxReached returns true if max records per writer are exceeded
func (w *writer) isMaxReached() bool {
	if w.max == 0 || w.count == 0 {
		return false
	}
	return w.count >= w.max
}

//isExpired returns last write exceeded writer rotation time
func (w *writer) isExpired(now time.Time) bool {
	if w.count == 0 {
		return false
	}
	if w.expiryTime == nil {
		return false
	}
	return w.expiryTime.Before(now)
}

//Write writes data
func (w *writer) Write(bs []byte) (n int, err error) {
	n, err = w.writer.Write(bs)
	return n, err
}

func (w *writer) increment() int {
	return int(atomic.AddInt32(&w.count, 1))

}

//Flush flushes log if needed
func (w *writer) Flush() error {
	return w.flusher.Flush()
}

func (w *writer) initRotation(rotation *config.Rotation, created time.Time, emitter *emitter.Service) {
	if rotation == nil {
		return
	}
	if w.rotationURL != "" && url.Scheme(w.destURL, file.Scheme) == file.Scheme {
		if url.Scheme(w.rotationURL, file.Scheme) == file.Scheme {
			w.rotationPath = url.Path(w.rotationURL)
		} else {
			_, name := url.Split(w.rotationURL, file.Scheme)
			w.rotationPath = path.Join(os.TempDir(), name)
			w.rotationTransfer = true
		}
	}
	w.expiryTime = rotation.ExpiryTime(created)
	w.max = int32(rotation.MaxEntries)
	if rotation.Emit != nil {
		w.emitter = emitter
	}
}

//NewWriter creates a writer
func newWriter(config *config.Stream, fs afs.Service, rotationURL string, index int, created time.Time, emitter *emitter.Service) (*writer, error) {
	writerCloser, err := fs.NewWriter(context.Background(), config.URL, file.DefaultFileOsMode)
	if err != nil {
		return nil, err
	}
	result := &writer{
		fs:          fs,
		index:       index,
		destURL:     config.URL,
		rotationURL: rotationURL,
		closer:      writerCloser,
		created:     created,
	}
	result.config = config

	if rotation := config.Rotation; rotation != nil {
		initRotation(result, rotation, created, emitter)
	}
	if config.IsGzip() {
		gzWriter := gzip.NewWriter(writerCloser)
		result.writer = gzWriter
		result.flusher = gzWriter

	} else {
		writer := bufio.NewWriter(writerCloser)
		result.writer = writer
		result.flusher = writer
	}
	return result, nil
}

func initRotation(result *writer, rotation *config.Rotation, created time.Time, emitter *emitter.Service) {
	if result.rotationURL != "" && url.Scheme(result.destURL, file.Scheme) == file.Scheme {
		if url.Scheme(result.rotationURL, file.Scheme) == file.Scheme {
			result.rotationPath = url.Path(result.rotationURL)
		} else {
			_, name := url.Split(result.rotationURL, file.Scheme)
			result.rotationPath = path.Join(os.TempDir(), name)
			result.rotationTransfer = true
		}
	}
	result.expiryTime = rotation.ExpiryTime(created)
	result.max = int32(rotation.MaxEntries)
	if rotation.Emit != nil {
		result.emitter = emitter
	}
}
