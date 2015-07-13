package tsm

import "time"

// Store defines an interface for saving time series
// attributes and data
type Store interface {
	WriteData(id, domain string, data *Data) error
	ReadData(id, domain string, start, end time.Time) (*Data, error)
	Describe(id, domain string) (*Description, error)
	Description() string
}
