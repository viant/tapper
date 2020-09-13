package buffer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewBytes(t *testing.T) {

	var useCases = []struct {
		description string
		items       []interface{}
		expect      string
	}{

		{
			description: "simply test",
			items: []interface{}{
				1,
				"test",
				2.3,
				true,
				uint(2),
			},
			expect: "1test2.3true2",
		},
		{
			description: "bytes test",
			items: []interface{}{
				[]byte("This is"),
				" a ",
				"test",
			},
			expect: "This is a test",
		},
	}

	for _, useCase := range useCases {
		bs := Pool.Borrow()
		for _, item := range useCase.items {
			switch v := item.(type) {
			case string:
				bs.AppendString(v)
			case int:
				bs.AppendInt(int64(v))
			case uint:
				bs.AppendUint(uint64(v))
			case float64:
				bs.AppendFloat(v, 64)
			case bool:
				bs.AppendBool(v)
			case []byte:
				bs.AppendBytes(v)
			}
		}
		assert.EqualValues(t, useCase.expect, string(bs.Bytes()))
		Pool.Put(bs)
	}

}
