package boltx

import (
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

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

// DeleteFromBucket removes the provided key from the provided bucket.
func DeleteFromBucket(db *bolt.DB, name, key []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(name)
		if bucket == nil {
			return nil
		}
		_ = bucket.Delete(key)
		return nil
	})
}
