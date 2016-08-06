# boltx [![Build Status](https://travis-ci.org/simia-tech/boltx.svg?branch=master)](https://travis-ci.org/simia-tech/boltx) [![Coverage Status](https://coveralls.io/repos/github/simia-tech/boltx/badge.svg?branch=master)](https://coveralls.io/github/simia-tech/boltx?branch=master) [![GoDoc](https://godoc.org/github.com/simia-tech/boltx?status.svg)](https://godoc.org/github.com/simia-tech/boltx) [![Go Report Card](https://goreportcard.com/badge/github.com/simia-tech/boltx)](https://goreportcard.com/report/github.com/simia-tech/boltx)


This package contains a collection of tools for [BoltDB](https://github.com/boltdb/bolt). It tries to simplify the
handling of models in a BoltDB bucket without being too opinionated.

It's basically assumes that models implement `encoding.BinaryMarshaler` and `encoding.BinaryUnmarshaler` from Go's
standard library.

```go
type model struct { ... }

func (m *model) MarshalBinary() ([]byte, error) { ... }

func (m *model) UnmarshalBinary([]byte) (error) { ... }
```

Those methods should handle the (de)serialization of the model. The interfaces are than used by the functions of
this package to store and load models.

```go
model := &model{}

boltx.PutModel(bucket, []byte("key"), model)

boltx.GetModel(bucket, []byte("key"), model)
```

## Deque

The `Deque` helper implements a deque (double-ended queue) on a bucket. It's persistent and safe to use with
multiple goroutines.

```go
deque := boltx.NewDeque(db, []byte("deque-test"))

go func () {
  for i := 0; i < 10; i++ {
    deque.EnqueueBack(&model{"item"})
  }
}()

model := &model{}
for found, _ := deque.DequeueFront(model); found; found, _ = deque.DequeueFront(model) {
  log.Println(model)
}
```
