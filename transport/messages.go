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
	ID        string                 `json:"id"`
	Type      Type                   `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Content   map[string]interface{} `json:"content"`
}

type CommandType string

const (
	CREATE  CommandType = "create"
	CONNECT CommandType = "connect"
	DESTROY CommandType = "destroy"
	EXECUTE CommandType = "execute"
	UPDATE  CommandType = "update"
)

type Command struct {
	Type   CommandType       `json:"type"`
	Source string            `json:"source"`
	Target string            `json:"target"`
	Params map[string]string `json:"params"`
}
