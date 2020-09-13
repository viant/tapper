package io

//Stream represents a message stream
type Stream interface {
	//Put puts  bytes
	Put(bs []byte)
	//Put puts a byte
	PutByte(b byte)
	//PutObject put stream encoded objects
	PutObject(key string, object Encoder)
	//PutObjects put objects
	PutObjects(key string, objects []Encoder)
	//Put puts string
	PutString(key, value string)
	//PutNonEmptyString puts non empty string
	PutNonEmptyString(key, value string)
	//PutB64EncodedBytes puts base64 encoded byte
	PutB64EncodedBytes(key string, bytes []byte)
	//PutStrings puts string slice
	PutStrings(key string, values []string)
	//PutInts puts ints slice
	PutInts(key string, values []int)
	//PutUInts puts uint slice
	PutUInts(key string, values []uint64)
	//PutInt puts int
	PutInt(key string, value int)
	//PutFloat puts float
	PutFloat(key string, value float64)
	//PutBool puts bool
	PutBool(key string, value bool)
}
