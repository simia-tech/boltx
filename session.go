package boltx

import (
	"sync"

	"github.com/boltdb/bolt"
)

// Session defines a update session. It can be used to re-used transactions that are waiting
// on an update.
type Session struct {
	db           *bolt.DB
	tx           *bolt.Tx
	updateSignal *sync.Cond
}

// NewSession returns a new initialized session.
func NewSession(db *bolt.DB) *Session {
	return &Session{
		db:           db,
		updateSignal: sync.NewCond(&sync.Mutex{}),
	}
}

// Update starts an update transaction on the db. If a transaction is set, it's re-used.
func (s *Session) Update(fn func(tx *bolt.Tx) error) error {
	if s.tx == nil {
		return s.db.Update(fn)
	}
	return fn(s.tx)
}
