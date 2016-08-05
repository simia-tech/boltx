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
		cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(&model{})
		for key, value, err := wrapper(cursor.First()); key != nil && err == nil; key, value, err = wrapper(cursor.Next()) {
			model := value.(*model)
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

		key, value, err := wrapper(cursor.First())
		require.NoError(t, err)
		assert.Nil(t, key)
		assert.Nil(t, value)
	})
}

func TestCursorIteratingOverInvalidElements(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		require.NoError(t, bucket.Put([]byte("test"), []byte("invalid")))

		cursor, wrapper := bucket.Cursor(), boltx.CursorWrapper(&model{})
		key, value, err := wrapper(cursor.First())
		assert.Equal(t, "test", string(key))
		assert.Equal(t, &model{field: "invalid"}, value)
		assert.Error(t, err)
	})
}
