package boltx

import (
	"encoding"
	"math"
	"math/big"

	"github.com/boltdb/bolt"
)

var middleKey = big.NewInt(0).SetUint64(math.MaxUint64 / 2)

// Deque defines a double-ended queue on a bucket. It's persistent and safe to use with
// multiple goroutines.
//
//   deque := boltx.NewDeque(db, []byte("deque-test"))
//
//   go func () {
//     for i := 0; i < 10; i++ {
//       deque.EnqueueBack(&model{"item"})
//     }
//   }()
//
//   model := &model{}
//   for found, _ := deque.DequeueFront(model); found; found, _ = deque.DequeueFront(model) {
//     log.Println(model)
//   }
type Deque struct {
	db      *bolt.DB
	name    []byte
	session *Session
}

// NewDeque initializes a deque in the bucket with the provided name.
func NewDeque(db *bolt.DB, name []byte) *Deque {
	return &Deque{
		db:      db,
		name:    name,
		session: NewSession(db),
	}
}

// EnqueueModelFront puts the provided model to the front of the deque.
func (d *Deque) EnqueueModelFront(model encoding.BinaryMarshaler) error {
	return d.session.Update(func(tx *bolt.Tx) error {
		return PushModelAndSignal(tx, d.name, PositionFront, model, DefaultUint64DequeKey, d.session)
	})
}

// EnqueueModelBack puts the provided model to the back of the deque.
func (d *Deque) EnqueueModelBack(model encoding.BinaryMarshaler) error {
	return d.session.Update(func(tx *bolt.Tx) error {
		return PushModelAndSignal(tx, d.name, PositionBack, model, DefaultUint64DequeKey, d.session)
	})
}

// DequeueModelFront gets the value from the front of the deque, unmarshals it into the provided
// model and removes it. If the deque is empty the call blocks until an element is enqueued.
func (d *Deque) DequeueModelFront(model encoding.BinaryUnmarshaler) error {
	return d.session.Update(func(tx *bolt.Tx) error {
		return PopModelOrWait(tx, d.name, PositionFront, model, d.session)
	})
}

// DequeueModelBack gets the value from the back of the deque, unmarshals it into the provided
// model and removes it. If the deque is empty the call blocks until an element is enqueued.
func (d *Deque) DequeueModelBack(model encoding.BinaryUnmarshaler) error {
	return d.session.Update(func(tx *bolt.Tx) error {
		return PopModelOrWait(tx, d.name, PositionBack, model, d.session)
	})
}

// Size returns the number of elements in the deque.
func (d *Deque) Size() int {
	return BucketSize(d.db, d.name)
}
