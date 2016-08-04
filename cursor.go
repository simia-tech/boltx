package boltx

import (
	"encoding"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

// Cursorx implements an extended cursor that unmarshals the selected values.
type Cursorx struct {
	bucket *bolt.Bucket
	cursor *bolt.Cursor
}

// Cursor returns an extended cursor on the provided bucket.
func Cursor(bucket *bolt.Bucket) *Cursorx {
	return &Cursorx{
		bucket: bucket,
		cursor: bucket.Cursor(),
	}
}

// Bucket returns the bucket that this cursor was created from.
func (c *Cursorx) Bucket() *bolt.Bucket {
	return c.bucket
}

// Delete removes the key/value under the cursor.
func (c *Cursorx) Delete() error {
	if err := c.cursor.Delete(); err != nil {
		return err
	}
	return nil
}

// First moves the cursor to the first element and unmarshals the value into the provided model.
func (c *Cursorx) First(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.First()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

// Last moves the cursor to the last element and unmarshals the value into the provided model.
func (c *Cursorx) Last(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Last()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

// Next moves the cursor to the next element and unmarshals the value into the provided model.
func (c *Cursorx) Next(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Next()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

// Prev moves the cursor to the previous element and unmarshals the value into the provided model.
func (c *Cursorx) Prev(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Prev()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

// Seek moves the cursor to the provided key and returns it. If the key does not exists, the next
// key is used. The corresponding value is unmarshaled into the provided model.
func (c *Cursorx) Seek(prefix []byte, model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Seek(prefix)
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}
