package boltx_test

import (
	"errors"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/simia-tech/boltx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForEachPick(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name := []byte("test")
	require.NoError(t, boltx.PutInBucket(db, name, []byte("test"), []byte("test")))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(name)

		key, value, err := boltx.ForEach(bucket, &model{}, func(key []byte, value interface{}) (boltx.Action, error) {
			if string(key) == "test" {
				return boltx.ActionReturn, nil
			}
			return boltx.ActionNone, nil
		})
		require.NoError(t, err)
		assert.Equal(t, "test", string(key))
		assert.Equal(t, &model{field: "test"}, value)

		return nil
	}))
}

func TestForEachUpdate(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name := []byte("test")
	require.NoError(t, boltx.PutInBucket(db, name, []byte("test 1"), []byte("test")))
	require.NoError(t, boltx.PutInBucket(db, name, []byte("test 2"), []byte("test")))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(name)

		count := 0
		key, value, err := boltx.ForEach(bucket, &model{}, func(key []byte, value interface{}) (boltx.Action, error) {
			count++
			model := value.(*model)
			model.field = "new test"
			return boltx.ActionUpdate | boltx.ActionReturn, nil
		})
		require.NoError(t, err)
		assert.Equal(t, "test 1", string(key))
		assert.Equal(t, &model{field: "new test"}, value)
		assert.Equal(t, 1, count)

		return nil
	}))

	model := &model{}
	found, err := boltx.GetModelFromBucket(db, name, []byte("test 1"), model)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "new test", model.field)
}

func TestForEachDelete(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name := []byte("test")
	require.NoError(t, boltx.PutInBucket(db, name, []byte("test 1"), []byte("test")))
	require.NoError(t, boltx.PutInBucket(db, name, []byte("test 2"), []byte("test")))

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(name)

		count := 0
		key, value, err := boltx.ForEach(bucket, &model{}, func(key []byte, value interface{}) (boltx.Action, error) {
			count++
			return boltx.ActionDelete, nil
		})
		require.NoError(t, err)
		assert.Nil(t, key)
		assert.Nil(t, value)
		assert.Equal(t, 2, count)

		return nil
	}))

	assert.Equal(t, 0, boltx.BucketSize(db, name))
}

func TestForEachErrors(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("test"))
		require.NoError(t, err)

		require.NoError(t, bucket.Put([]byte("test"), []byte("invalid")))

		_, _, err = boltx.ForEach(bucket, &model{}, func(key []byte, value interface{}) (boltx.Action, error) {
			return boltx.ActionNone, nil
		})
		assert.Error(t, err)

		require.NoError(t, bucket.Put([]byte("test"), []byte("test")))

		_, _, err = boltx.ForEach(bucket, &model{}, func(key []byte, value interface{}) (boltx.Action, error) {
			model := value.(*model)
			model.field = "invalid"
			return boltx.ActionUpdate, nil
		})
		assert.Error(t, err)

		_, _, err = boltx.ForEach(bucket, &modelWithoutMarshaler{}, func(key []byte, value interface{}) (boltx.Action, error) {
			return boltx.ActionUpdate, nil
		})
		assert.Error(t, err)

		_, _, err = boltx.ForEach(bucket, &modelWithoutMarshaler{}, func(key []byte, value interface{}) (boltx.Action, error) {
			return boltx.ActionNone, errors.New("error")
		})
		assert.Error(t, err)

		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("test"))

		_, _, err := boltx.ForEach(bucket, &model{}, func(key []byte, value interface{}) (boltx.Action, error) {
			return boltx.ActionDelete, nil
		})
		assert.Error(t, err)

		return nil
	}))
}
