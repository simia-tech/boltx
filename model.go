package boltx

import (
	"encoding"
	"fmt"

	"github.com/boltdb/bolt"
)

// PutModel marshals the provided model and stores it in the provided bucket under the provided key.
func PutModel(bucket *bolt.Bucket, key []byte, model encoding.BinaryMarshaler) error {
	value, err := model.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshaling failed: %v", err)
	}

	if err := bucket.Put(key, value); err != nil {
		return fmt.Errorf("put failed: %v", err)
	}

	return nil
}

// GetModel loads the value from the provided bucket at the provided key and unmarshals it into the
// provided model. If the value was found, true is returned. False otherwise.
func GetModel(bucket *bolt.Bucket, key []byte, model encoding.BinaryUnmarshaler) (bool, error) {
	value := bucket.Get(key)
	if len(value) == 0 {
		return false, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return false, fmt.Errorf("unmarshaling failed: %v", err)
	}

	return true, nil
}

// PutModelInBucket marshals the provided model, creates the bucket with the provided name if it's
// not existing and stores the marshalled model under the provided key.
func PutModelInBucket(db *bolt.DB, name, key []byte, model encoding.BinaryMarshaler) error {
	value, err := model.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshaling failed: %v", err)
	}
	return PutInBucket(db, name, key, value)
}

// GetModelFromBucket loads the value from the provided bucket at the provided key and unmarshals it
// into the provided model. If the bucket and the value was found, true is returned. False otherwise.
func GetModelFromBucket(db *bolt.DB, name, key []byte, model encoding.BinaryUnmarshaler) (bool, error) {
	value := GetFromBucket(db, name, key)
	if value == nil {
		return false, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return false, fmt.Errorf("unmarshaling failed: %v", err)
	}

	return true, nil
}
