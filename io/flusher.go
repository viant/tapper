package io

//Flusher represents flusher
type Flusher interface {
	//Flush flush data stream if needed
	Flush() error
}
