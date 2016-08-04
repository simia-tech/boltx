package boltx_test

type model struct {
	field string
}

func (m *model) MarshalBinary() ([]byte, error) {
	return []byte(m.field), nil
}

func (m *model) UnmarshalBinary(data []byte) error {
	m.field = string(data)
	return nil
}
