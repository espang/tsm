## tsm

Time series managment daemon.

## Motivation

Motivation for this project...

## Time series

Time series are represented by following attributes:


```go
type Frequency struct {
	Steps int
	Unit  GridType
}

type TsAttribute struct {
	Id       string
	SoY      *StartOfYear
	Freq     Frequency
	Location *time.Location
	Periodic bool
}
```
