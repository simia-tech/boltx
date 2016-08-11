package boltx_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/boltx"
)

func TestDequeQueueing(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	assert.Error(t, deque.EnqueueModelBack(&model{field: "invalid"}))
	require.NoError(t, deque.EnqueueModelBack(&model{field: "test"}))

	assert.Equal(t, 1, deque.Size())

	value := &model{}
	require.NoError(t, deque.DequeueModelFront(value))
	assert.Equal(t, &model{field: "test"}, value)

	assert.Equal(t, 0, deque.Size())

	require.NoError(t, boltx.PutInBucket(db, []byte("test"), []byte("test"), []byte("invalid")))
	assert.Error(t, deque.DequeueModelFront(value))
}

func TestDequeReverseQueueing(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	assert.Error(t, deque.EnqueueModelFront(&model{field: "invalid"}))
	require.NoError(t, deque.EnqueueModelFront(&model{field: "test"}))

	assert.Equal(t, 1, deque.Size())

	value := &model{}
	require.NoError(t, deque.DequeueModelBack(value))
	assert.Equal(t, &model{field: "test"}, value)

	assert.Equal(t, 0, deque.Size())

	require.NoError(t, boltx.PutInBucket(db, []byte("test"), []byte("test"), []byte("invalid")))
	assert.Error(t, deque.DequeueModelBack(value))
}

func TestDequeOrdering(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	require.NoError(t, deque.EnqueueModelFront(&model{field: "two"}))
	require.NoError(t, deque.EnqueueModelFront(&model{field: "one"}))
	require.NoError(t, deque.EnqueueModelBack(&model{field: "three"}))

	model := &model{}
	require.NoError(t, deque.DequeueModelFront(model))
	assert.Equal(t, "one", model.field)

	require.NoError(t, deque.DequeueModelFront(model))
	assert.Equal(t, "two", model.field)

	require.NoError(t, deque.DequeueModelFront(model))
	assert.Equal(t, "three", model.field)
}

func TestDequeDequeueFrontOnEmptyDequeAndEnqueueBack(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	values := make(chan *model)
	go func() {
		value := &model{}
		require.NoError(t, deque.DequeueModelFront(value))
		values <- value
	}()

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, deque.EnqueueModelBack(&model{field: "test"}))

	assert.Equal(t, &model{field: "test"}, <-values)
}

func TestDequeWithInvalidBucketName(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte(""))

	assert.Error(t, deque.EnqueueModelFront(&model{field: "test"}))
}
