package boltx

import (
	"encoding"
	"math"
	"math/big"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
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
	db     *bolt.DB
	name   []byte
	notify chan struct{}
}

// NewDeque initializes a deque in the bucket with the provided name.
func NewDeque(db *bolt.DB, name []byte) *Deque {
	return &Deque{
		db:   db,
		name: name,
	}
}

// EnqueueFront puts the provided model to the front of the deque.
func (d *Deque) EnqueueFront(model encoding.BinaryMarshaler) error {
	return d.enqueue(model, -1, func(cursor *bolt.Cursor) ([]byte, []byte) {
		return cursor.First()
	})
}

// EnqueueBack puts the provided model to the back of the deque.
func (d *Deque) EnqueueBack(model encoding.BinaryMarshaler) error {
	return d.enqueue(model, 1, func(cursor *bolt.Cursor) ([]byte, []byte) {
		return cursor.Last()
	})
}

// DequeueFront gets the value from the front of the deque, unmarshals it into the provided
// model and removes it. If the deque is empty the call blocks until an element is enqueued.
func (d *Deque) DequeueFront(model encoding.BinaryUnmarshaler) (bool, error) {
	return d.dequeue(model, func(cursor *bolt.Cursor) ([]byte, []byte) {
		return cursor.First()
	})
}

// DequeueBack gets the value from the back of the deque, unmarshals it into the provided
// model and removes it. If the deque is empty the call blocks until an element is enqueued.
func (d *Deque) DequeueBack(model encoding.BinaryUnmarshaler) (bool, error) {
	return d.dequeue(model, func(cursor *bolt.Cursor) ([]byte, []byte) {
		return cursor.Last()
	})
}

func (d *Deque) enqueue(model encoding.BinaryMarshaler, delta int64, fn func(*bolt.Cursor) ([]byte, []byte)) error {
	tx, _ := d.db.Begin(true)
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists(d.name)
	if err != nil {
		return errors.Wrapf(err, "bucket [%s] creation failed", d.name)
	}

	cursor := bucket.Cursor()
	key, _ := fn(cursor)
	if key == nil {
		key = middleKey.Bytes()
	}
	key = addToKey(key, delta)

	if err := PutModel(bucket, key, model); err != nil {
		return err
	}

	if d.notify != nil {
		d.notify <- struct{}{}
		d.notify = nil
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit failed")
	}

	return nil
}

func (d *Deque) dequeue(model encoding.BinaryUnmarshaler, fn func(*bolt.Cursor) ([]byte, []byte)) (bool, error) {
	tx, _ := d.db.Begin(true)
	defer tx.Rollback()

	var bucket *bolt.Bucket
	for bucket = tx.Bucket(d.name); isEmptyBucket(bucket); bucket = tx.Bucket(d.name) {
		d.notify = make(chan struct{})
		_ = tx.Rollback()
		<-d.notify
		tx, _ = d.db.Begin(true)
	}

	cursor := bucket.Cursor()
	_, value := fn(cursor)

	if err := model.UnmarshalBinary(value); err != nil {
		return false, errors.Wrap(err, "unmarshaling failed")
	}
	_ = cursor.Delete()
	if err := tx.Commit(); err != nil {
		return true, errors.Wrap(err, "commit failed")
	}
	return true, nil
}

func addToKey(key []byte, value int64) []byte {
	return big.NewInt(0).Add(big.NewInt(0).SetBytes(key), big.NewInt(value)).Bytes()
}

func isEmptyBucket(bucket *bolt.Bucket) bool {
	if bucket == nil {
		return true
	}
	key, _ := bucket.Cursor().First()
	return key == nil
}
