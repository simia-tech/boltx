package boltx

import (
	"fmt"

	"github.com/boltdb/bolt"
)

// PutInBucket creates the bucket with the provided name if it's not existing and stores the
// provided value under the provided key.
func PutInBucket(db *bolt.DB, name, key, value []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(name)
		if err != nil {
			return fmt.Errorf("bucket [%s] creation failed: %v", name, err)
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

// BucketSize returns the number of key/value pairs in the provided bucket. If the bucket doesn't
// exists, 0 is returned.
func BucketSize(db *bolt.DB, name []byte) int {
	tx, _ := db.Begin(false)
	defer tx.Rollback()

	bucket := tx.Bucket(name)
	if bucket == nil {
		return 0
	}

	return bucket.Stats().KeyN
}
