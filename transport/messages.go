package transport

import (
	"time"
)

type Type int32

const (
	COMMAND Type = 0
	REPORT  Type = 1
)

type Message struct {
	ID        string    `json:"id"`
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Content   string    `json:"content"`
}
