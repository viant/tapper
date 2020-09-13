package buffer

import (
	"io"
	"strconv"
	"time"
)

const expandSize = 1024

//Bytes represents buffer bytes
type Bytes struct {
	buf   []byte
	index int
	size  int
}

//Size return size
func (b *Bytes) Size() int {
	return b.index
}

//WriteTo writes to writer
func (b *Bytes) WriteTo(w io.Writer) (int64, error) {
	if b.index == 0 {
		return 0, nil
	}
	offset := 0
	for {
		n, err := w.Write(b.buf[offset:b.index])
		offset += n
		if err != nil || offset >= b.index {
			return int64(offset), err
		}
	}
}

//AppendBytes append bytes
func (b *Bytes) AppendBytes(bs []byte) {
	bsLen := len(bs)
	if bsLen == 0 {
		return
	}
	if bsLen+b.index >= len(b.buf) {
		size := 1024 * b.index
		if size < bsLen {
			size = bsLen
		}
		b.buf = append(b.buf, make([]byte, size)...)
	}
	copy(b.buf[b.index:], bs)
	b.index += bsLen
}

//AppendByte append a byte
func (b *Bytes) AppendByte(bs byte) {
	if b.index+1 >= len(b.buf) {
		newBuffer := make([]byte, 1024)
		b.buf = append(b.buf, newBuffer...)
	}
	b.buf[b.index] = bs
	b.index++
}

//AppendString append a string
func (b *Bytes) AppendString(s string) {
	sLen := len(s)
	if sLen == 0 {
		return
	}
	if sLen+b.index >= len(b.buf) {
		size := expandSize
		if size < sLen {
			size = sLen
		}
		newBuffer := make([]byte, size)
		b.buf = append(b.buf, newBuffer...)
	}
	//sourceBytes := []byte(s)
	//copy(b.buf[b.index:], sourceBytes[:sLen])
	copy(b.buf[b.index:], s)
	b.index += sLen
}

//Bytes return message bytes
func (b *Bytes) Bytes() []byte {
	return b.buf[:b.index]
}

// AppendInt appends an integer to the underlying buffer (assuming base 10).
func (b *Bytes) AppendInt(i int64) {
	s := strconv.FormatInt(i, 10)
	b.AppendString(s)
}

// AppendTime appends the time formatted using the specified layout.
func (b *Bytes) AppendTime(t time.Time, layout string) {
	s := t.Format(layout)
	b.AppendString(s)
}

// AppendUint appends an unsigned integer to the underlying buffer (assuming
// base 10).
func (b *Bytes) AppendUint(i uint64) {
	s := strconv.FormatUint(i, 10)
	b.AppendString(s)
}

// AppendBool appends a bool to the underlying buffer.
func (b *Bytes) AppendBool(v bool) {
	s := strconv.FormatBool(v)
	b.AppendString(s)
}

// AppendFloat appends a float to the underlying buffer.
func (b *Bytes) AppendFloat(f float64, bitSize int) {
	s := strconv.FormatFloat(f, 'f', -1, bitSize)
	b.AppendString(s)
}

// Trim trims any final character from the buffer
func (b *Bytes) Trim(ch byte) {
	if b.buf[b.index-1] == ch && b.index > 0 {
		b.index--
	}
}

//Reset reset index
func (b *Bytes) Reset() {
	b.index = 0
	b.buf = b.buf[:b.size]
}

//NewBytes creates bytes
func NewBytes(size int) *Bytes {
	return &Bytes{
		buf:  make([]byte, size),
		size: size,
	}
}
