package boltx_test

import "errors"

type model struct {
	field string
}

func (m *model) MarshalBinary() ([]byte, error) {
	if m.field == "invalid" {
		return nil, errors.New("marshaling error")
	}
	return []byte(m.field), nil
}

func (m *model) UnmarshalBinary(data []byte) error {
	m.field = string(data)
	if m.field == "invalid" {
		return errors.New("unmarshaling error")
	}
	return nil
}
