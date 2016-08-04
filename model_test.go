package boltx_test

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/simia-tech/boltx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutAndGet(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "test"}
		require.NoError(t, boltx.Put(bucket, key, value))

		model := &model{}
		found, err := boltx.Get(bucket, key, model)
		require.NoError(t, err)

		assert.True(t, found)
		assert.Equal(t, "test", model.field)
	})
}

func TestPutAndGetOfInvalidModel(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "invalid"}
		assert.Error(t, boltx.Put(bucket, key, value))

		require.NoError(t, bucket.Put(key, []byte("invalid")))
		model := &model{}
		_, err := boltx.Get(bucket, key, model)
		assert.Error(t, err)
	})
}

func TestPutInReadOnlyBucket(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inReadOnlyTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "test"}
		assert.Error(t, boltx.Put(bucket, key, value))
	})
}

func TestGetOfMissingModel(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		model := &model{}
		found, err := boltx.Get(bucket, []byte("missing"), model)
		require.NoError(t, err)

		assert.False(t, found)
		assert.Equal(t, "", model.field)
	})
}
