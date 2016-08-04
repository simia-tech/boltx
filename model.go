package boltx

import (
	"encoding"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

// Put marshals the provided model and stores it in the provided bucket under the provided key.
func Put(bucket *bolt.Bucket, key []byte, model encoding.BinaryMarshaler) error {
	value, err := model.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshaling failed")
	}

	if err := bucket.Put(key, value); err != nil {
		return errors.Wrap(err, "put failed")
	}

	return nil
}

// Get loads the value from the provided bucket at the provided key and unmarshals it into the
// provided model.
func Get(bucket *bolt.Bucket, key []byte, model encoding.BinaryUnmarshaler) (bool, error) {
	value := bucket.Get(key)
	if len(value) == 0 {
		return false, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return false, errors.Wrap(err, "unmarshaling failed")
	}

	return true, nil
}
