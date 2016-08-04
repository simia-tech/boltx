package boltx_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/require"
)

func setUpTestDB(tb testing.TB) (*bolt.DB, func()) {
	file, err := ioutil.TempFile("", "bolt-")
	require.NoError(tb, err)
	require.NoError(tb, file.Close())

	db, err := bolt.Open(file.Name(), 0600, nil)
	require.NoError(tb, err)

	return db, func() {
		require.NoError(tb, db.Close())
		require.NoError(tb, os.Remove(file.Name()))
	}
}

func inTestBucket(tb testing.TB, db *bolt.DB, fn func(*bolt.Bucket)) {
	require.NoError(tb, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("test"))
		require.NoError(tb, err)

		fn(bucket)

		return nil
	}))
}

func inReadOnlyTestBucket(tb testing.TB, db *bolt.DB, fn func(*bolt.Bucket)) {
	require.NoError(tb, db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("test"))
		require.NoError(tb, err)
		return nil
	}))

	require.NoError(tb, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("test"))
		require.NotNil(tb, bucket)

		fn(bucket)

		return nil
	}))
}
