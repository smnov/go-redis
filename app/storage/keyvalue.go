package storage

import (
	"strconv"
	"time"
)

type KeyValue struct {
	data map[string]string
}

type StorageError struct {
	s string
}

func (e *StorageError) Error() string {
	return e.s
}

func NewKeyValue() KeyValue {
	return KeyValue{
		data: make(map[string]string),
	}
}

func (s *KeyValue) SetVariable(k, v string, args map[string]string) error {
	px, ok := args["px"]
	if ok {
		t, err := strconv.Atoi(px)
		if err != nil {
			return err
		}
		timer := time.NewTimer(time.Duration(int64(t)) * time.Millisecond)
		go func() {
			<-timer.C
			s.DeleteVariable(k)
		}()
	}
	s.data[k] = v
	return nil
}

func (s *KeyValue) GetVariable(key string) (string, error) {
	v, ok := s.data[key]
	if !ok {
		return "", &StorageError{"key not found"}
	}
	return v, nil
}

func (s *KeyValue) DeleteVariable(key string) {
	delete(s.data, key)
}
