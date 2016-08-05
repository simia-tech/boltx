package boltx

import (
	"encoding"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

// PutModel marshals the provided model and stores it in the provided bucket under the provided key.
func PutModel(bucket *bolt.Bucket, key []byte, model encoding.BinaryMarshaler) error {
	value, err := model.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshaling failed")
	}

	if err := bucket.Put(key, value); err != nil {
		return errors.Wrap(err, "put failed")
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
		return false, errors.Wrap(err, "unmarshaling failed")
	}

	return true, nil
}

// PutInBucket creates the bucket with the provided name if it's not existing and stores the
// provided value under the provided key.
func PutInBucket(db *bolt.DB, name, key, value []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(name)
		if err != nil {
			return errors.Wrapf(err, "bucket [%s] creation failed", name)
		}
		return bucket.Put(key, value)
	})
}

// GetFromBucket loads the value from the provided bucket at the provided key. If the bucket and
// the value was found, the content is returned. Nil otherwise.
func GetFromBucket(db *bolt.DB, name, key []byte) []byte {
	result := []byte(nil)
	_ = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(name)
		if bucket == nil {
			return nil
		}
		result = bucket.Get(key)
		return nil
	})
	return result
}

// PutModelInBucket marshals the provided model, creates the bucket with the provided name if it's
// not existing and stores the marshalled model under the provided key.
func PutModelInBucket(db *bolt.DB, name, key []byte, model encoding.BinaryMarshaler) error {
	value, err := model.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshaling failed")
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
		return false, errors.Wrap(err, "unmarshaling failed")
	}

	return true, nil
}
