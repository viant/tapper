package encoder

import (
	"fmt"
	"github.com/viant/tapper/io"
	"github.com/viant/xunsafe"
	"reflect"
	"time"
)

const (
	intMaks     = uint16(1) << 1
	float64Mask = uint16(1) << 2
	stringMask  = uint16(1) << 3
	boolMask    = uint16(1) << 4
	timePtrMask = uint16(1) << 5
	float32Mask = uint16(1) << 6
	timeMask    = uint16(1) << 7
	stringsMask = uint16(1) << 8
	intsMask    = uint16(1) << 9
)

//Provider represents a struct encoder provider
type Provider struct {
	reflect.Type
	Int     []*xunsafe.Field
	Float64 []*xunsafe.Field
	String  []*xunsafe.Field
	Bool    []*xunsafe.Field
	TimePtr []*xunsafe.Field
	Float32 []*xunsafe.Field
	Time    []*xunsafe.Field
	Strings []*xunsafe.Field
	Ints    []*xunsafe.Field
	mask    uint16
}

//New creates encoder for a struct value
func (p *Provider) New(value interface{}) io.Encoder {
	return &Struct{
		Provider: p,
		ptr:      xunsafe.AsPointer(value),
		value:    value,
	}
}

//New creates struct encoder provider
func New(value interface{}) (*Provider, error) {
	var sType reflect.Type
	switch actual := value.(type) {
	case reflect.Type:
		sType = actual
	default:
		sType = reflect.TypeOf(value)
		if sType.Kind() == reflect.Ptr {
			sType = sType.Elem()
		}
	}
	result := &Provider{Type: sType}
	xStruct := xunsafe.NewStruct(sType)
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		switch field.Kind() {
		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
			result.mask |= intMaks
			result.Int = append(result.Int, field)
		case reflect.Float64:
			result.mask |= float64Mask
			result.Float64 = append(result.Float64, field)
		case reflect.String:
			result.mask |= stringMask
			result.String = append(result.String, field)
		case reflect.Bool:
			result.mask |= boolMask
			result.Bool = append(result.Bool, field)
		case reflect.Float32:
			result.mask |= float32Mask
			result.Float32 = append(result.Float32, field)
		case reflect.Slice:
			switch field.Elem().Kind() {
			case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
				result.mask |= intsMask
				result.Ints = append(result.Ints, field)
			case reflect.String:
				result.mask |= stringsMask
				result.Strings = append(result.Strings, field)
			default:
				return nil, fmt.Errorf("not yet supported type: %v", field.Type.String())
			}
		default:
			if field.Type.AssignableTo(timeType) {
				result.mask |= timeMask
				result.Time = append(result.Time, field)
				continue
			}
			if field.Type.AssignableTo(timePtrType) {
				result.mask |= timePtrMask
				result.TimePtr = append(result.TimePtr, field)
				continue
			}
			return nil, fmt.Errorf("not yet supported type: %v", field.Type.String())
		}
	}

	return result, nil
}

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})
