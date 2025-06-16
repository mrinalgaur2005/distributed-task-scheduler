package model

type Task struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Priority int                    `json:"priority"`
	Payload  map[string]interface{} `json:"payload"`
}
