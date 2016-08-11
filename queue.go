package boltx

import (
	"encoding"

	"github.com/boltdb/bolt"
)

// Queue defines a single-ended queue on a bucket. It's persistent and safe to use with
// multiple goroutines.
//
//   queue := boltx.NewQueue(db, []byte("queue-test"))
//
//   queue.EnqueueModel(&model{"item"})
//
//   model := &model{}
//   queue.DequeueModel(model)
//
//   log.Println(model)
type Queue struct {
	db      *bolt.DB
	name    []byte
	session *Session
}

// NewQueue initializes a queue in the bucket with the provided name.
func NewQueue(db *bolt.DB, name []byte) *Queue {
	return &Queue{
		db:      db,
		name:    name,
		session: NewSession(db),
	}
}

// EnqueueModel puts the provided model to the back of the queue.
func (q *Queue) EnqueueModel(model encoding.BinaryMarshaler) error {
	return q.session.Update(func(tx *bolt.Tx) error {
		return PushModelAndSignal(tx, q.name, PositionBack, model, DefaultUint64DequeKey, q.session)
	})
}

// DequeueModel gets the value from the front of the deque, unmarshals it into the provided
// model and removes it. If the deque is empty the call blocks until an element is enqueued.
func (q *Queue) DequeueModel(model encoding.BinaryUnmarshaler) error {
	return q.session.Update(func(tx *bolt.Tx) error {
		return PopModelOrWait(tx, q.name, PositionFront, model, q.session)
	})
}

// Size returns the number of elements in the deque.
func (q *Queue) Size() int {
	return BucketSize(q.db, q.name)
}
