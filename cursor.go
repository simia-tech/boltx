package boltx

import (
	"encoding"

	"github.com/pkg/errors"
)

// CursorWrapper returns a function that can be used to wrap calls to bolt.Cursor.
//
//   model := &model{}
//   cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(model)
//   for key, err := wrapper(cursor.First()); key != nil && err == nil; key, err = wrapper(cursor.Next()) {
//     fmt.Println(model.field)
//   }
func CursorWrapper(model encoding.BinaryUnmarshaler) func([]byte, []byte) ([]byte, error) {
	return func(key, value []byte) ([]byte, error) {
		if key == nil {
			return nil, nil
		}

		if err := model.UnmarshalBinary(value); err != nil {
			return key, errors.Wrap(err, "unmarshaling failed")
		}

		return key, nil
	}
}
