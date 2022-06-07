package csv

import (
	"github.com/viant/tapper/buffer"
	"github.com/viant/tapper/io"
	"github.com/viant/tapper/msg"
	"github.com/viant/toolbox"
	iow "io"
	"log"
	"sync/atomic"
)

//CsvMessage represents transaction CsvMessage
type Message struct {
	bs             *buffer.Bytes
	provider       *msg.Provider
	borrowed       int32
	sliceDelimiter string
	useQuote       bool
}

const (
	defaultSliceDelimiter = ":"
)

//Begin begin CsvMessage
func (m *Message) Begin() {
	return
}

//PutByte put bytes
func (m *Message) PutByte(b byte) {
	m.bs.AppendByte(b)
}

func (m *Message) quoted(key string) {
	m.appendQuote()
	m.bs.AppendString(key)
	m.appendQuote()
}

func (m *Message) next() {
	m.bs.AppendString(",")
}

//Put put bytes
func (m *Message) Put(bs []byte) {
	m.bs.AppendBytes(bs)
}

//PutB64EncodedBytes puts ky and bas64 encoded values
func (m *Message) PutB64EncodedBytes(key string, bytes []byte) {
	log.Panic("PutB64EncodedBytes is not supported for CSV message")
}

//PutObject not supported for csv message
func (m *Message) PutObject(key string, object io.Encoder) {
	log.Panic("PutObject is not supported for CSV message")
}

//PutObjects put objects
func (m *Message) PutObjects(key string, objects []io.Encoder) {
	log.Panic("PutObjects is not supported for CSV message")
}

//PutNonEmptyString put key and non empty value
func (m *Message) PutNonEmptyString(key, value string) {
	if len(value) == 0 {
		return
	}
	m.PutString(key, value)
}

//PutString put key and string value
func (m *Message) PutString(key, value string) {
	m.quoted(value)
	m.next()
}

func (m *Message) appendQuote() {
	if m.useQuote {
		m.bs.AppendByte('"')
	}
}

//PutStrings put key and string slice
func (m *Message) PutStrings(key string, values []string) {
	m.appendQuote()
	for i, value := range values {
		if i > 0 {
			m.Put([]byte(value))
			m.Put([]byte(m.getSliceDelimiter()))
		}
	}
	m.bs.Trim(m.getSliceDelimiter()[0])
	m.appendQuote()
	m.next()
}

//PutInts puts key and int slice
func (m *Message) PutInts(key string, values []int) {
	for i, value := range values {
		if i > 0 {
			m.Put([]byte(toolbox.AsString(value)))
			m.Put([]byte(m.getSliceDelimiter()))
		}
	}
	m.bs.Trim(m.getSliceDelimiter()[0])
	m.next()
}

//PutUInts put key and uint slice
func (m *Message) PutUInts(key string, values []uint64) {
	for i, value := range values {
		if i > 0 {
			m.Put([]byte(toolbox.AsString(value)))
			m.Put([]byte(m.getSliceDelimiter()))
		}
	}
	m.bs.Trim(m.getSliceDelimiter()[0])
	m.next()
}

//PutInt put key and int value
func (m *Message) PutInt(key string, value int) {
	m.bs.AppendInt(int64(value))
	m.next()
}

//PutFloat put key and float value
func (m *Message) PutFloat(key string, value float64) {
	m.bs.AppendFloat(value, 64)
	m.next()

}

//PutBool put key and bool value
func (m *Message) PutBool(key string, value bool) {
	m.PutString(key, toolbox.AsString(value))
}

//WriteTo writes CsvMessage to the writer
func (m *Message) WriteTo(w iow.Writer) (int64, error) {
	m.end()
	return m.bs.WriteTo(w)
}

//End end CsvMessage
func (m *Message) End() {
	m.bs.Trim(',')
}

func (m *Message) end() {
	m.End()
	m.bs.AppendBytes([]byte("\n"))
}

//Free returns bytes to the pool
func (m *Message) Free() {
	m.provider.Put(m)
}

func (m *Message) SetBorrowed() {
	atomic.StoreInt32(&m.borrowed, 1)
}

func (m *Message) CompareAndSwap() bool{
	return atomic.CompareAndSwapInt32(&m.borrowed, 1, 0)
}

func (m *Message) GetByteBuffer() *buffer.Bytes {
	return m.bs
}

// SetDelimiter sets delimiter for csv slice
func (m *Message) SetSliceDelimiter(delimiter string) {
	m.sliceDelimiter = delimiter
}

func (m *Message) getSliceDelimiter() string {
	if m.sliceDelimiter == "" {
		return defaultSliceDelimiter
	}
	return m.sliceDelimiter
}

//UseQuotes applicable to only csv
func (m *Message) UseQuotes(quote bool) {
	m.useQuote = quote
}

func New(provider *msg.Provider,  bytes *buffer.Bytes) msg.Message {
	return &Message{
		bs:       bytes,
		provider: provider,
	}
}


