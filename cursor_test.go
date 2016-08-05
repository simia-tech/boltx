package boltx_test

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/boltx"
)

func TestCursorIterating(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		require.NoError(t, boltx.Put(bucket, []byte("test"), &model{field: "test"}))

		count := 0
		model := &model{}
		cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(model)
		for key, err := wrapper(cursor.First()); key != nil && err == nil; key, err = wrapper(cursor.Next()) {
			assert.Equal(t, "test", model.field)
			count++
		}
		assert.Equal(t, 1, count)
	})
}

func TestCursorIteratingInEmptyBucket(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(nil)

		key, err := wrapper(cursor.First())
		require.NoError(t, err)
		assert.Nil(t, key)
	})
}

func TestCursorIteratingOverInvalidElements(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		require.NoError(t, bucket.Put([]byte("test"), []byte("invalid")))

		model := &model{}
		cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(model)
		key, err := wrapper(cursor.First())
		assert.Equal(t, "test", string(key))
		assert.Error(t, err)
	})
}
