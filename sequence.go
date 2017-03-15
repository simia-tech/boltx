package boltx

import (
	"encoding"
	"fmt"
	"math"
	"math/big"

	"github.com/boltdb/bolt"
)

var (
	// DefaultUint64QueueKey defines the default key for a queue with uint64 keys.
	DefaultUint64QueueKey = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	// DefaultUint64DequeKey defines the default key for a deque (double-ended queue) with uint64 keys.
	DefaultUint64DequeKey = big.NewInt(0).SetUint64(math.MaxUint64 / 2).Bytes()

	// PositionFront specifies the front of a queue or deque.
	PositionFront = &Position{delta: -1, fn: func(cursor *bolt.Cursor) ([]byte, []byte) {
		return cursor.First()
	}}

	// PositionBack specifies the back of a queue or deque.
	PositionBack = &Position{delta: 1, fn: func(cursor *bolt.Cursor) ([]byte, []byte) {
		return cursor.Last()
	}}
)

// Position defines a position in a queue or deque.
type Position struct {
	delta int64
	fn    func(*bolt.Cursor) ([]byte, []byte)
}

// Push inserts the provided value at the provided position in the provided bucket. If the
// bucket is empty, the provided defaultKey is used.
func Push(tx *bolt.Tx, name []byte, position *Position, value, defaultKey []byte) error {
	bucket, err := tx.CreateBucketIfNotExists(name)
	if err != nil {
		return err
	}

	cursor := bucket.Cursor()
	key, _ := position.fn(cursor)
	if key == nil {
		key = defaultKey
	} else {
		key = addToKey(key, position.delta)
	}

	if err := bucket.Put(key, value); err != nil {
		return err
	}

	return nil
}

// Pop removes and returns the value at the provided position in the provided bucket. If the bucket is empty,
// nil is returned.
func Pop(tx *bolt.Tx, name []byte, position *Position) []byte {
	bucket := tx.Bucket(name)
	if bucket == nil {
		return nil
	}

	cursor := bucket.Cursor()
	_, value := position.fn(cursor)
	_ = cursor.Delete()
	return value
}

func addToKey(key []byte, value int64) []byte {
	return big.NewInt(0).Add(big.NewInt(0).SetBytes(key), big.NewInt(value)).Bytes()
}

// PopOrWait tries to pop a value from the provided bucket at the provided position. If the bucket is empty,
// the provided transaction is stored in the provided session and the function blocks until a value is inserted
// into the bucket. Insert-transactions should be started with session.Update.
func PopOrWait(tx *bolt.Tx, name []byte, position *Position, session *Session) []byte {
	session.updateSignal.L.Lock()
	session.tx = tx
	var value []byte
	for value = Pop(tx, name, position); value == nil; value = Pop(tx, name, position) {
		session.updateSignal.Wait()
	}
	session.tx = nil
	session.updateSignal.L.Unlock()
	return value
}

// PushAndSignal pushes the the provided value at the provided position in the provided bucket. Afterwards
// an update is siganled through the provided session.
func PushAndSignal(tx *bolt.Tx, name []byte, position *Position, value, defaultKey []byte, session *Session) error {
	session.updateSignal.L.Lock()
	defer session.updateSignal.L.Unlock()

	if err := Push(tx, name, position, value, defaultKey); err != nil {
		return err
	}
	session.updateSignal.Signal()

	return nil
}

// PopModelOrWait behaves like PopOrWair, but handels the model unmarshaling.
func PopModelOrWait(
	tx *bolt.Tx,
	name []byte,
	position *Position,
	model encoding.BinaryUnmarshaler,
	session *Session,
) error {
	value := PopOrWait(tx, name, position, session)

	if err := model.UnmarshalBinary(value); err != nil {
		return fmt.Errorf("unmarshaling failed: %v", err)
	}

	return nil
}

// PushModelAndSignal behaves like PushAndSignal, but handels the model marshaling.
func PushModelAndSignal(
	tx *bolt.Tx,
	name []byte,
	position *Position,
	model encoding.BinaryMarshaler,
	defaultKey []byte,
	session *Session,
) error {
	value, err := model.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshaling failed: %v", err)
	}

	if err := PushAndSignal(tx, name, position, value, defaultKey, session); err != nil {
		return err
	}

	return nil
}
