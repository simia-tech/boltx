package boltx

import (
	"encoding"
	"reflect"

	"github.com/pkg/errors"
)

// CursorWrapper returns a function that can be used to wrap calls to bolt.Cursor. The wrapper creates
// a new instance of the provided prototype and unmarshals the value into it.
//
//   cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(&model{})
//   for key, value, err := wrapper(cursor.First()); key != nil && err == nil; key, value, err = wrapper(cursor.Next()) {
//     model := value.(*model)
//     fmt.Println(model.field)
//   }
func CursorWrapper(prototype encoding.BinaryUnmarshaler) func([]byte, []byte) ([]byte, encoding.BinaryUnmarshaler, error) {
	return func(key, value []byte) ([]byte, encoding.BinaryUnmarshaler, error) {
		if key == nil {
			return nil, nil, nil
		}

		t := reflect.ValueOf(prototype).Type()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		model := reflect.New(t).Interface().(encoding.BinaryUnmarshaler)

		if err := model.UnmarshalBinary(value); err != nil {
			return key, model, errors.Wrap(err, "unmarshaling failed")
		}

		return key, model, nil
	}
}
