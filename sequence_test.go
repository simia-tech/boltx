package boltx_test

import (
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/boltx"
)

func TestPushAndPop(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name := []byte("test")

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, boltx.Push(tx, name, boltx.PositionBack, []byte("two"), boltx.DefaultUint64DequeKey))
		require.NoError(t, boltx.Push(tx, name, boltx.PositionBack, []byte("three"), boltx.DefaultUint64DequeKey))
		require.NoError(t, boltx.Push(tx, name, boltx.PositionFront, []byte("one"), boltx.DefaultUint64DequeKey))

		assert.Equal(t, "one", string(boltx.Pop(tx, name, boltx.PositionFront)))
		assert.Equal(t, "two", string(boltx.Pop(tx, name, boltx.PositionFront)))
		assert.Equal(t, "three", string(boltx.Pop(tx, name, boltx.PositionBack)))
		return nil
	}))
}

func TestPushWithInvalidDefaultKey(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name := []byte("test")

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		assert.Error(t, boltx.Push(tx, name, boltx.PositionBack, []byte("test"), nil))
		return nil
	}))
}

func TestPushAndPopConcurrently(t *testing.T) {
	db, tearDown := setUpTestDB(t)
	defer tearDown()

	name := []byte("test")
	session := boltx.NewSession(db)

	result := make(chan string)
	go func() {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			result <- string(boltx.PopOrWait(tx, name, boltx.PositionFront, session))
			return nil
		}))
	}()

	time.Sleep(10 * time.Millisecond)

	require.NoError(t, session.Update(func(tx *bolt.Tx) error {
		require.NoError(t, boltx.PushAndSignal(tx, name, boltx.PositionFront, []byte("test"), boltx.DefaultUint64DequeKey, session))
		return nil
	}))

	assert.Equal(t, "test", <-result)
	close(result)

	require.Equal(t, 0, boltx.BucketSize(db, name))

	require.NoError(t, session.Update(func(tx *bolt.Tx) error {
		require.NoError(t, boltx.PushAndSignal(tx, name, boltx.PositionBack, []byte("test"), boltx.DefaultUint64QueueKey, session))
		assert.Equal(t, "test", string(boltx.PopOrWait(tx, name, boltx.PositionFront, session)))
		return nil
	}))

	require.Equal(t, 0, boltx.BucketSize(db, name))
}
