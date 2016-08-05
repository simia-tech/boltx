package boltx_test

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/simia-tech/boltx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutModelAndGetModel(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "test"}
		require.NoError(t, boltx.PutModel(bucket, key, value))

		found, err := boltx.GetModel(bucket, key, value)
		require.NoError(t, err)

		assert.True(t, found)
		assert.Equal(t, "test", value.field)
	})
}

func TestPutModelAndGetModelOfInvalidModel(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "invalid"}
		assert.Error(t, boltx.PutModel(bucket, key, value))

		require.NoError(t, bucket.Put(key, []byte("invalid")))
		_, err := boltx.GetModel(bucket, key, value)
		assert.Error(t, err)
	})
}

func TestPutModelInReadOnlyBucket(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inReadOnlyTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "test"}
		assert.Error(t, boltx.PutModel(bucket, key, value))
	})
}

func TestGetModelOfMissingModel(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		value := &model{}
		found, err := boltx.GetModel(bucket, []byte("missing"), value)
		require.NoError(t, err)

		assert.False(t, found)
		assert.Equal(t, "", value.field)
	})
}

func TestPutModelInBucketAndGetModelFromBucket(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name, key := []byte("test"), []byte("test")

	value := &model{field: "test"}
	assert.Error(t, boltx.PutModelInBucket(db, []byte(""), key, value))
	assert.Error(t, boltx.PutModelInBucket(db, name, key, &model{field: "invalid"}))
	require.NoError(t, boltx.PutModelInBucket(db, name, key, value))

	found, err := boltx.GetModelFromBucket(db, []byte("missing"), key, value)
	require.NoError(t, err)
	assert.False(t, found)

	found, err = boltx.GetModelFromBucket(db, name, key, value)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "test", value.field)

	require.NoError(t, boltx.PutInBucket(db, name, key, []byte("invalid")))
	found, err = boltx.GetModelFromBucket(db, name, key, value)
	assert.False(t, found)
	assert.Error(t, err)
}
