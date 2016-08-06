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

	assert.Error(t, deque.EnqueueBack(&model{field: "invalid"}))
	require.NoError(t, deque.EnqueueBack(&model{field: "test"}))

	assert.Equal(t, 1, deque.Size())

	value := &model{}
	found, err := deque.DequeueFront(value)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, &model{field: "test"}, value)

	assert.Equal(t, 0, deque.Size())

	require.NoError(t, boltx.PutInBucket(db, []byte("test"), []byte("test"), []byte("invalid")))
	found, err = deque.DequeueFront(value)
	assert.Error(t, err)
	assert.False(t, found)
}

func TestDequeReverseQueueing(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	assert.Error(t, deque.EnqueueFront(&model{field: "invalid"}))
	require.NoError(t, deque.EnqueueFront(&model{field: "test"}))

	assert.Equal(t, 1, deque.Size())

	value := &model{}
	found, err := deque.DequeueBack(value)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, &model{field: "test"}, value)

	assert.Equal(t, 0, deque.Size())

	require.NoError(t, boltx.PutInBucket(db, []byte("test"), []byte("test"), []byte("invalid")))
	found, err = deque.DequeueBack(value)
	assert.Error(t, err)
	assert.False(t, found)
}

func TestDequeOrdering(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	require.NoError(t, deque.EnqueueFront(&model{field: "two"}))
	require.NoError(t, deque.EnqueueFront(&model{field: "one"}))
	require.NoError(t, deque.EnqueueBack(&model{field: "three"}))

	model := &model{}
	found, err := deque.DequeueFront(model)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "one", model.field)

	found, err = deque.DequeueFront(model)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "two", model.field)

	found, err = deque.DequeueFront(model)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "three", model.field)
}

func TestDequeDequeueFrontOnEmptyDequeAndEnqueueBack(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))

	values := make(chan *model)
	go func() {
		value := &model{}
		found, err := deque.DequeueFront(value)
		require.NoError(t, err)
		assert.True(t, found)
		values <- value
	}()

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, deque.EnqueueBack(&model{field: "test"}))

	assert.Equal(t, &model{field: "test"}, <-values)
}

func TestDequeWithInvalidBucketName(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte(""))

	assert.Error(t, deque.EnqueueFront(&model{field: "test"}))
}

func TestDequeEnqueueFrontWithCommitProblems(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))
	require.NoError(t, deque.EnqueueFront(&model{field: "test"}))
	deque.ReadOnly = true

	assert.Error(t, deque.EnqueueFront(&model{field: "test"}))
}

func TestDequeDequeueFrontWithCommitProblems(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	deque := boltx.NewDeque(db, []byte("test"))
	require.NoError(t, deque.EnqueueFront(&model{field: "test"}))
	deque.ReadOnly = true

	_, err := deque.DequeueFront(&model{field: "test"})
	assert.Error(t, err)
}
