package json

import (
	"encoding/base64"
	"github.com/viant/tapper/buffer"
	"github.com/viant/tapper/io"
	"github.com/viant/tapper/msg"
	"github.com/viant/toolbox"
	iow "io"
	"sync/atomic"
)

//Message represents transaction message
type Message struct {
	bs       *buffer.Bytes
	provider *msg.Provider
	borrowed int32
}

//Begin begin message
func (m *Message) Begin() {
	m.bs.AppendString("{")
}

//PutByte put bytes
func (m *Message) PutByte(b byte) {
	m.bs.AppendByte(b)
}

func (m *Message) quoted(key string) {
	m.bs.AppendByte('"')
	m.bs.AppendString(key)
	m.bs.AppendByte('"')
}

func (m *Message) next() {
	m.bs.AppendString(",")
}

func (m *Message) key(key string) {
	m.quoted(key)
	m.bs.AppendString(":")
}

//Put put bytes
func (m *Message) Put(bs []byte) {
	m.bs.AppendBytes(bs)
}

//PutB64EncodedBytes puts ky and bas64 encoded values
func (m *Message) PutB64EncodedBytes(key string, bytes []byte) {
	text := base64.StdEncoding.EncodeToString(bytes)
	m.PutString(key, text)
}

//PutObject put encoded object
func (m *Message) PutObject(key string, object io.Encoder) {
	m.key(key)
	m.Begin()
	object.Encode(m)
	m.End()
	m.next()
}

//PutObjects put objects
func (m *Message) PutObjects(key string, objects []io.Encoder) {
	m.key(key)
	m.Put([]byte("["))
	for i, object := range objects {
		if i > 0 {
			m.next()
		}
		m.Begin()
		object.Encode(m)
		m.End()
	}
	m.Put([]byte("]"))
	m.next()
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
	m.key(key)
	m.quoted(value)
	m.next()
}

//PutStrings put key and string slice
func (m *Message) PutStrings(key string, values []string) {
	m.key(key)
	m.Put([]byte("["))

	for i, value := range values {
		if i > 0 {
			m.next()
		}
		m.quoted(value)
	}
	m.Put([]byte("]"))
	m.next()
}

//PutInts puts key and int slice
func (m *Message) PutInts(key string, values []int) {
	m.key(key)
	m.Put([]byte("["))

	for i, value := range values {
		if i > 0 {
			m.next()
		}
		m.Put([]byte(toolbox.AsString(value)))
	}
	m.Put([]byte("]"))
	m.next()
}

//PutUInts put key and uint slice
func (m *Message) PutUInts(key string, values []uint64) {
	m.key(key)
	m.Put([]byte("["))
	for i, value := range values {
		if i > 0 {
			m.next()
		}
		m.Put([]byte(toolbox.AsString(value)))
	}
	m.Put([]byte("]"))
	m.next()
}

//PutInt put key and int value
func (m *Message) PutInt(key string, value int) {
	m.key(key)
	m.bs.AppendInt(int64(value))
	m.next()
}

//PutFloat put key and float value
func (m *Message) PutFloat(key string, value float64) {
	m.key(key)
	m.bs.AppendFloat(value, 64)
	m.next()

}

//PutFloats put key and float values
func (m *Message) PutFloats(key string, values []float64) {
	m.key(key)
	m.Put([]byte("["))
	for i, value := range values {
		if i > 0 {
			m.next()
		}
		m.bs.AppendFloat(value, 64)
	}
	m.Put([]byte("]"))
	m.next()
}

//PutBool put key and bool value
func (m *Message) PutBool(key string, value bool) {
	m.key(key)
	m.bs.AppendBool(value)
	m.next()
}

//PutBools put key and bool values
func (m *Message) PutBools(key string, values []bool) {
	m.key(key)
	m.Put([]byte("["))
	for i, value := range values {
		if i > 0 {
			m.next()
		}
		m.bs.AppendBool(value)
	}
	m.Put([]byte("]"))
	m.next()
}

//WriteTo writes message to the writer
func (m *Message) WriteTo(w iow.Writer) (int64, error) {
	m.end()
	return m.bs.WriteTo(w)
}

//End end message
func (m *Message) End() {
	m.bs.Trim(',')
	m.bs.AppendString("}")
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

func (m *Message) CompareAndSwap() bool {
	return atomic.CompareAndSwapInt32(&m.borrowed, 1, 0)
}

func (m *Message) GetByteBuffer() *buffer.Bytes {
	return m.bs
}

func (m *Message) SetSliceDelimiter(delimiter string) {
	return
}

//UseQuotes applicable to only csv
func (m *Message) UseQuotes(quote bool) {
	return
}

func New(provider *msg.Provider, bytes *buffer.Bytes) msg.Message {
	return &Message{
		bs:       bytes,
		provider: provider,
	}
}
