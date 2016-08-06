package boltx_test

import (
	"testing"

	"github.com/simia-tech/boltx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutInBucketAndGetFromBucket(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name, key, value := []byte("test"), []byte("test"), []byte("test")

	assert.Error(t, boltx.PutInBucket(db, []byte(""), key, value))
	require.NoError(t, boltx.PutInBucket(db, name, key, value))

	value = boltx.GetFromBucket(db, []byte("missing"), key)
	assert.Nil(t, value)

	value = boltx.GetFromBucket(db, name, key)
	assert.Equal(t, "test", string(value))
}

func TestDeleteFromBucket(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name, key, value := []byte("test"), []byte("test"), []byte("test")
	require.NoError(t, boltx.PutInBucket(db, name, key, value))

	assert.NoError(t, boltx.DeleteFromBucket(db, []byte("missing"), key))
	assert.NoError(t, boltx.DeleteFromBucket(db, name, key))
}

func TestBucketSize(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name, key, value := []byte("test"), []byte("test"), []byte("test")
	require.NoError(t, boltx.PutInBucket(db, name, key, value))

	assert.Equal(t, 0, boltx.BucketSize(db, []byte("missing")))

	assert.Equal(t, 1, boltx.BucketSize(db, name))
	require.NoError(t, boltx.DeleteFromBucket(db, name, key))
	assert.Equal(t, 0, boltx.BucketSize(db, name))
}
