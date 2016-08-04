package boltx_test

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/boltx"
)

func TestCursorIteratingForward(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		require.NoError(t, boltx.Put(bucket, []byte("test"), &model{field: "test"}))

		cursor := boltx.Cursorx(bucket)
		assert.Equal(t, bucket, cursor.Bucket())

		count := 0
		model := &model{}
		for key, err := cursor.First(model); key != nil && err == nil; key, err = cursor.Next(model) {
			assert.Equal(t, "test", model.field)
			count++
		}
		assert.Equal(t, 1, count)
	})
}

func TestCursorIteratingBackward(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		require.NoError(t, boltx.Put(bucket, []byte("test"), &model{field: "test"}))

		cursor := boltx.Cursorx(bucket)
		assert.Equal(t, bucket, cursor.Bucket())

		count := 0
		model := &model{}
		for key, err := cursor.Last(model); key != nil && err == nil; key, err = cursor.Prev(model) {
			assert.Equal(t, "test", model.field)
			count++
		}
		assert.Equal(t, 1, count)
	})
}

func TestCursorSeekAndDelete(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		require.NoError(t, boltx.Put(bucket, []byte("test"), &model{field: "test"}))

		cursor := boltx.Cursorx(bucket)
		assert.Equal(t, bucket, cursor.Bucket())

		model := &model{}
		key, err := cursor.Seek([]byte("t"), model)
		require.NoError(t, err)
		assert.Equal(t, "test", string(key))
		assert.Equal(t, "test", model.field)

		require.NoError(t, cursor.Delete())

		found, err := boltx.Get(bucket, []byte("test"), model)
		require.NoError(t, err)
		assert.False(t, found)
	})
}
