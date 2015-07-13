package tsm

import "time"

type Grid int

const (
	Minute Grid = iota
	Hour
	Day
)

// Freq represents
type Freq struct {
	Steps int
	Grid  Grid
	Tz    *time.Location
}

// Attribute
type Attribute struct {
	Id       string
	Domain   string
	Desc     string
	Freq     Freq
	Periodic bool
}

type Data struct {
	Times  []time.Time
	Values []float64
}

type Description struct {
	First  time.Time
	Last   time.Time
	Count  int
	Min    float64
	Max    float64
	Mean   float64
	Std    float64
	Median float64
}
