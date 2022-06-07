package msg

import (
	"github.com/viant/tapper/buffer"
	"sync"
)


//Provider represents a message provider
type Provider struct {
	pool       *sync.Pool
	bufferSize int
}

//NewMessage creates a message
func (p *Provider) NewMessage() Message {
	message := p.pool.Get().(Message)
	message.SetBorrowed()
	message.Begin()
	return message
}

func (p *Provider) Put(m Message) {
	if !m.CompareAndSwap() {
		return
	}
	m.GetByteBuffer().Reset()
	p.pool.Put(m)
}

//NewProvider creates a message provider with supplied buffer size and pool size
func NewProvider(messageSize, concurrency int, newMessage func(provider *Provider,  bytes *buffer.Bytes) Message) *Provider {
	provider := &Provider{
		bufferSize: messageSize,
		pool:       &sync.Pool{},
	}
	provider.pool.New = func() interface{} {
		return newMessage(provider, buffer.NewBytes(messageSize))
	}
	for i := 0; i < concurrency; i++ {
		provider.Put(provider.NewMessage())
	}
	return provider
}
