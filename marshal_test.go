package boltx_test

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/simia-tech/boltx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type model struct {
	field string
}

func (m *model) MarshalBinary() ([]byte, error) {
	return []byte(m.field), nil
}

func (m *model) UnmarshalBinary(data []byte) error {
	m.field = string(data)
	return nil
}

func TestPutAndGet(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		key, value := []byte("test"), &model{field: "test"}
		require.NoError(t, boltx.Put(bucket, key, value))

		model := &model{}
		require.NoError(t, boltx.Get(bucket, key, model))

		assert.Equal(t, "test", model.field)
	})
}

func TestGetOfMissingModel(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	inTestBucket(t, db, func(bucket *bolt.Bucket) {
		model := &model{}
		require.NoError(t, boltx.Get(bucket, []byte("missing"), model))

		assert.Equal(t, "", model.field)
	})
}
