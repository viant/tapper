package encoder

import (
	"github.com/viant/tapper/io"
	"time"
	"unsafe"
)

//Struct reprsents basic struct encoder
type Struct struct {
	*Provider
	ptr   unsafe.Pointer
	value interface{}
}

//Encode encodes a stream
func (e *Struct) Encode(stream io.Stream) {
	e.encodeInt(stream)
	e.encodeFloat64(stream)
	e.encodeString(stream)
	e.encodeBool(stream)
	e.encodeTimePtr(stream)
	if e.mask < float32Mask {
		return
	}
	e.encodeFloat32(stream)
	e.encodeTime(stream)
	e.encodeInts(stream)
	e.encodeStrings(stream)
}

func (e *Struct) encodeInt(stream io.Stream) {
	if len(e.Int) == 0 {
		return
	}
	for _, f := range e.Int {
		v := f.Int(e.ptr)
		stream.PutInt(f.Name, v)
	}
}

func (e *Struct) encodeInts(stream io.Stream) {
	if len(e.Ints) == 0 {
		return
	}
	for _, f := range e.Ints {
		v := f.Addr(e.ptr).(*[]int)
		stream.PutInts(f.Name, *v)
	}
}

func (e *Struct) encodeStrings(stream io.Stream) {
	if len(e.Strings) == 0 {
		return
	}
	for _, f := range e.Strings {
		v := f.Addr(e.ptr).(*[]string)
		stream.PutStrings(f.Name, *v)
	}
}

func (e *Struct) encodeTimePtr(stream io.Stream) {
	if len(e.TimePtr) == 0 {
		return
	}
	for _, f := range e.TimePtr {
		v := f.TimePtr(e.ptr)
		if v == nil {
			continue
		}
		stream.PutString(f.Name, v.Format(time.RFC3339))
	}
}

func (e *Struct) encodeTime(stream io.Stream) {
	if len(e.Time) == 0 {
		return
	}
	for _, f := range e.Time {
		v := f.Time(e.ptr)
		stream.PutString(f.Name, v.Format(time.RFC3339))
	}
}

func (e *Struct) encodeString(stream io.Stream) {
	if len(e.String) == 0 {
		return
	}
	for _, f := range e.String {
		v := f.String(e.ptr)
		stream.PutNonEmptyString(f.Name, v)
	}
}

func (e *Struct) encodeBool(stream io.Stream) {
	if len(e.Bool) == 0 {
		return
	}
	for _, f := range e.Bool {
		v := f.Bool(e.ptr)
		stream.PutBool(f.Name, v)
	}
}

func (e *Struct) encodeFloat64(stream io.Stream) {
	if len(e.Float64) == 0 {
		return
	}
	for _, f := range e.Float64 {
		v := f.Float64(e.ptr)
		stream.PutFloat(f.Name, v)
	}
}

func (e *Struct) encodeFloat32(stream io.Stream) {
	if len(e.Float32) == 0 {
		return
	}
	for _, f := range e.Float32 {
		v := f.Float32(e.ptr)
		stream.PutFloat(f.Name, float64(v))
	}
}
