package boltx_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/boltx"
)

func TestQueueQueueing(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	queue := boltx.NewQueue(db, []byte("test"))

	assert.Error(t, queue.EnqueueModel(&model{field: "invalid"}))
	require.NoError(t, queue.EnqueueModel(&model{field: "test"}))

	assert.Equal(t, 1, queue.Size())

	value := &model{}
	require.NoError(t, queue.DequeueModel(value))
	assert.Equal(t, &model{field: "test"}, value)

	assert.Equal(t, 0, queue.Size())

	require.NoError(t, boltx.PutInBucket(db, []byte("test"), []byte("test"), []byte("invalid")))
	assert.Error(t, queue.DequeueModel(value))
}

func TestQueueDequeueOnEmpty(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	queue := boltx.NewQueue(db, []byte("test"))

	values := make(chan *model)
	go func() {
		value := &model{}
		require.NoError(t, queue.DequeueModel(value))
		values <- value
	}()

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, queue.EnqueueModel(&model{field: "test"}))

	assert.Equal(t, &model{field: "test"}, <-values)
}
