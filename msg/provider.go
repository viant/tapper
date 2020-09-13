package msg

import (
	"github.com/viant/tapper/buffer"
	"sync"
	"sync/atomic"
)

//Provider represents a message provider
type Provider struct {
	pool       *sync.Pool
	bufferSize int
}

//NewMessage creates a message
func (p *Provider) NewMessage() *Message {
	message := p.pool.Get().(*Message)
	atomic.StoreInt32(&message.borrowed, 1)
	message.Begin()
	return message
}

func (p *Provider) put(m *Message) {
	if !atomic.CompareAndSwapInt32(&m.borrowed, 1, 0) {
		return
	}
	m.bs.Reset()
	p.pool.Put(m)
}

//NewProvider creates a message provider with supplied buffer size and pool size
func NewProvider(messageSize, concurrency int) *Provider {
	provider := &Provider{
		bufferSize: messageSize,
		pool:       &sync.Pool{},
	}
	provider.pool.New = func() interface{} {
		return &Message{
			bs:       buffer.NewBytes(messageSize),
			provider: provider,
		}
	}

	for i := 0; i < concurrency; i++ {
		provider.put(provider.NewMessage())
	}
	return provider
}
