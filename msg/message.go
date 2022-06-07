package msg

import (
	"github.com/viant/tapper/buffer"
	"github.com/viant/tapper/io"
	iow "io"
)

type Message interface {
	io.Stream
	Begin()
	End()
	SetBorrowed()
	CompareAndSwap() bool
	GetByteBuffer() *buffer.Bytes
	SetSliceDelimiter(delimiter string)
	WriteTo(w iow.Writer) (int64, error)
	Free()
	UseQuotes(quote bool)
}
