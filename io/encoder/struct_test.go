package encoder_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/tapper/io/encoder"
	"github.com/viant/tapper/msg"
	"strings"
	"testing"
)

func TestStruct_Encode(t *testing.T) {

	type Foo struct {
		ID   int
		Name string
	}

	type Bar struct {
		ID   int
		Name string
		F    float64
		B    bool
		Desc string
		V    []int
	}

	var testCases = []struct {
		description string
		value       interface{}
		expect      string
	}{
		{
			description: "basic type",
			value:       &Foo{ID: 123, Name: "123"},
			expect:      `{"ID":123,"Name":"123"}`,
		},
		{
			description: "type with basic repeated",
			value:       &Bar{ID: 2, Name: "Bob", F: 1.3, B: true, V: []int{1, 2}},
			expect:      `{"ID":2,"F":1.3,"Name":"Bob","B":true,"V":[1,2]}`,
		},
	}

	provider := msg.NewProvider(1024, 1)
	for _, testCase := range testCases {
		msg := provider.NewMessage()
		provider, _ := encoder.New(testCase.value)
		stream := provider.New(testCase.value)
		stream.Encode(msg)
		buf := new(bytes.Buffer)
		msg.WriteTo(buf)
		msg.Free()
		if !assert.Equal(t, testCase.expect, strings.TrimSpace(buf.String())) {
			fmt.Print(buf.String())
		}

	}

}
