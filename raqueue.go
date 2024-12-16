package raqueue

import (
	"encoding/json"
)

type RAQueue struct {
	msgIndex uint
	totalMsg uint
	capacity uint
	filepath string
}

type Option struct {
	Capacity uint
}

var raQueueClient *RAQueue

func New(option *Option) *RAQueue {
	if raQueueClient == nil {
		raQueueClient = &RAQueue{
			msgIndex: 0,
			totalMsg: 0,
			capacity: max(1, option.Capacity),
		}
	}
	return raQueueClient
}

func (r *RAQueue) Send(v interface{}) (bool, error) {
	_, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RAQueue) Read(v interface{}) error {
	if err := json.Unmarshal([]byte{}, v); err != nil {
		return err
	}
	return nil
}
