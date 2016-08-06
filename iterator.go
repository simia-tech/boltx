package boltx

import (
	"encoding"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

const (
	// ActionNone indicates a noop.
	ActionNone = 1 << iota

	// ActionReturn indicates that the iteration should be stopped and the current element should be returned.
	ActionReturn = 1 << iota

	// ActionUpdate tells the iterator that the current element has been changed and should be updated.
	ActionUpdate = 1 << iota

	// ActionDelete tells the iterator to delete the current element.
	ActionDelete = 1 << iota
)

// Action indicates how an element should be handled after the iterator went over it.
type Action int

// ForEach iterates over all elements in the bucket.
func ForEach(
	bucket *bolt.Bucket,
	prototype encoding.BinaryUnmarshaler,
	fn func([]byte, interface{}) (Action, error),
) ([]byte, interface{}, error) {
	t := reflect.ValueOf(prototype).Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	cursor := bucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		model := reflect.New(t).Interface().(encoding.BinaryUnmarshaler)

		if err := model.UnmarshalBinary(value); err != nil {
			return nil, nil, errors.Wrap(err, "unmarshaling failed")
		}

		action, err := fn(key, model)
		if err != nil {
			return nil, nil, err
		}

		if ActionDelete&action != 0 {
			if err := cursor.Delete(); err != nil {
				return nil, nil, err
			}
		} else if ActionUpdate&action != 0 {
			bm, ok := model.(encoding.BinaryMarshaler)
			if !ok {
				return nil, nil, errors.Errorf("prototype %T has to implement encoding.BinaryMarshaler in order to update", prototype)
			}
			if err := PutModel(bucket, key, bm); err != nil {
				return nil, nil, err
			}
		}

		if ActionReturn&action != 0 {
			return key, model, nil
		}
	}

	return nil, nil, nil
}
