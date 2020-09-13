package io

//Encoder defines stream encoder
type Encoder interface {
	//Encode encodes a stream
	Encode(stream Stream)
}
