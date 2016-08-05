// Package boltx contains a collection of tools for [BoltDB](https://github.com/boltdb/bolt). It tries to simplify the
// handling of models in a BoltDB bucket without being too opinionated.
//
// It's basically assumes that models implement `encoding.BinaryMarshaler` and `encoding.BinaryUnmarshaler` from Go's
// standard library.
//
//   type model struct { ... }
//
//   func (m *model) MarshalBinary() ([]byte, error) { ... }
//
//   func (m *model) UnmarshalBinary([]byte) (error) { ... }
//
// Those methods should handle the (de)serialization of the model. The interfaces are than used by the functions of
// this package to store and load models.
//
//   model := &model{}
//
//   boltx.Put(bucket, []byte("key"), model)
//
//   boltx.Get(bucket, []byte("key"), model)
package boltx
