package report

import "encoding/json"

type Status struct {
	Success bool
	Error   string
}
type Progress struct {
	Status
}

func (p *Progress) String() string {
	bytes, err := json.Marshal(p)
	if err != nil {
		return "Error: " + err.Error()
	}
	return string(bytes)
}
