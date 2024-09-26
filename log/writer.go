package log

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
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

// writer represents an optimized writer
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
	loggerClose      bool
}

func (w *writer) isClosed() bool {
	return atomic.LoadInt32(&w.closed) == 1
}

// Close closes this writer
func (w *writer) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	if w.rotationURL == "" {
		if w.count == 0 {
			return nil
		}
		err := w.flusher.Flush()
		if writerCloser, ok := w.writer.(io.Closer); ok && err == nil {
			err = writerCloser.Close()
		}
		return w.closer.Close()
	}

	if w.rotationPath != "" {
		src := url.Path(w.destURL)
		if err := os.Rename(src, w.rotationPath); err != nil {
			return errors.Wrapf(err, "failed to rename: %v to %v", src, w.rotationPath)
		}
	}
	if w.loggerClose {
		if err := w.closeQuietly(); err != nil {
			log.Print(err)
			return err
		}
		return nil
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
		if w.count == 0 {
			if w.rotationPath != "" {
				w.fs.Delete(ctx, w.rotationPath)
			}
			return nil
		}
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
	source, reader, err := w.sourceReader(ctx)
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

func (w *writer) sourceReader(ctx context.Context) (string, io.ReadCloser, error) {
	source := w.rotationURL
	if w.rotationPath != "" {
		source = w.rotationPath
	}
	reader, err := w.fs.OpenURL(ctx, source)
	if err != nil {
		return "", nil, err
	}
	return source, reader, nil
}

// isMaxReached returns true if max records per writer are exceeded
func (w *writer) isMaxReached() bool {
	if w.max == 0 || w.count == 0 {
		return false
	}
	return w.count >= w.max
}

// isExpired returns last write exceeded writer rotation time
func (w *writer) isExpired(now time.Time) bool {
	if w.count == 0 {
		return false
	}
	if w.expiryTime == nil {
		return false
	}
	return w.expiryTime.Before(now)
}

// Write writes data
func (w *writer) Write(bs []byte) (n int, err error) {
	n, err = w.writer.Write(bs)
	return n, err
}

func (w *writer) increment() int {
	return int(atomic.AddInt32(&w.count, 1))

}

// Flush flushes log if needed
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

func (w *writer) merge(ctx context.Context, from *writer) error {
	if err := from.Flush(); err != nil {
		return fmt.Errorf("failed to merge loggers: flush: %w", err)
	}
	sourceURL, reader, err := from.sourceReader(context.Background())
	if err != nil {
		return fmt.Errorf("failed to merge loggers: %w", err)
	}
	defer reader.Close()
	if _, err = w.writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to merge loggers: newLine %w", err)
	}
	if _, err = io.Copy(w.writer, reader); err != nil {
		return fmt.Errorf("failed to merge loggers: copy %w", err)
	}
	atomic.CompareAndSwapInt32(&from.closed, 0, 1)
	_ = w.fs.Delete(ctx, sourceURL)
	return nil
}

// NewWriter creates a writer
func newWriter(config *config.Stream, fs afs.Service, rotationURL string, index int, created time.Time, emitter *emitter.Service) (*writer, error) {

	var options = make([]storage.Option, 0)
	if config.StreamUpload {
		options = append(options, option.NewSkipChecksum(true))
	}

	writerCloser, err := fs.NewWriter(context.Background(), config.URL, file.DefaultFileOsMode, options...)
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
