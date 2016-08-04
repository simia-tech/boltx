package boltx

import (
	"encoding"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

// Cursor defines the cursor interface.
type Cursor interface {
	Bucket() *bolt.Bucket
	Delete() error
	First(encoding.BinaryUnmarshaler) ([]byte, error)
	Last(encoding.BinaryUnmarshaler) ([]byte, error)
	Next(encoding.BinaryUnmarshaler) ([]byte, error)
	Prev(encoding.BinaryUnmarshaler) ([]byte, error)
	Seek([]byte, encoding.BinaryUnmarshaler) ([]byte, error)
}

type cursor struct {
	bucket *bolt.Bucket
	cursor *bolt.Cursor
}

// Cursorx returns a cursor on the provided bucket.
func Cursorx(bucket *bolt.Bucket) Cursor {
	return &cursor{
		bucket: bucket,
		cursor: bucket.Cursor(),
	}
}

func (c *cursor) Bucket() *bolt.Bucket {
	return c.bucket
}

func (c *cursor) Delete() error {
	if err := c.cursor.Delete(); err != nil {
		return err
	}
	return nil
}

func (c *cursor) First(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.First()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

func (c *cursor) Last(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Last()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

func (c *cursor) Next(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Next()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

func (c *cursor) Prev(model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Prev()
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}

func (c *cursor) Seek(prefix []byte, model encoding.BinaryUnmarshaler) ([]byte, error) {
	key, value := c.cursor.Seek(prefix)
	if key == nil {
		return nil, nil
	}

	if err := model.UnmarshalBinary(value); err != nil {
		return key, errors.Wrap(err, "unmarshaling failed")
	}

	return key, nil
}
