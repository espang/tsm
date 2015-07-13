package tsm

import (
	"errors"
	"fmt"
	"log"
	"math"
	"testing"
	"time"
)

var store Store

func init() {
	path := tempfile()
	st, err := NewBoltStore(path)
	if err != nil {
		log.Fatal(err)
	}
	store = st
}

func TestWriteRead(t *testing.T) {
	d := genTimeseries(24)
	id := uuid()
	domain := "testing"

	err := store.WriteData(id, domain, d)
	if err != nil {
		t.Errorf("Error writing %s", err)
	}

	start := d.Times[0]
	end := d.Times[len(d.Times)-1].Add(1 * time.Second)
	rd, err := store.ReadData(id, domain, start, end)

	err = compare(d, rd)
	if err != nil {
		t.Errorf("Read values not equal to written %s", err)
	}
}

func TestWriteDescribe1(t *testing.T) {
	n := 24
	d := genTimeseries(n)
	id := uuid()
	domain := "testing"

	err := store.WriteData(id, domain, d)
	if err != nil {
		t.Errorf("Error writing %s", err)
	}

	start := d.Times[0]
	end := d.Times[len(d.Times)-1]

	desc, err := store.Describe(id, domain)
	if err != nil {
		t.Errorf("Error describe %s", err)
	}

	total := 0.
	mean := 11.5
	for _, val := range d.Values {
		total += math.Pow(val-mean, 2)
	}
	std := math.Sqrt(total / float64(n-1))
	sdesc := &Description{
		Count:  n,
		First:  start,
		Last:   end,
		Min:    0.,
		Max:    23.,
		Mean:   11.5,
		Median: 11.5,
		Std:    std,
	}
	err = describe(desc, sdesc)
	if err != nil {
		t.Errorf("Read values not equal to written %s", err)
	}
}

func TestWriteDescribe2(t *testing.T) {
	n := 25
	d := genTimeseries(n)
	id := uuid()
	domain := "testing"

	err := store.WriteData(id, domain, d)
	if err != nil {
		t.Errorf("Error writing %s", err)
	}

	start := d.Times[0]
	end := d.Times[len(d.Times)-1]

	desc, err := store.Describe(id, domain)
	if err != nil {
		t.Errorf("Error describe %s", err)
	}

	total := 0.
	mean := 12.
	for _, val := range d.Values {
		total += math.Pow(val-mean, 2)
	}
	std := math.Sqrt(total / float64(n-1))
	sdesc := &Description{
		Count:  n,
		First:  start,
		Last:   end,
		Min:    0.,
		Max:    24.,
		Mean:   12,
		Median: 12,
		Std:    std,
	}
	err = describe(desc, sdesc)
	if err != nil {
		t.Errorf("Read values not equal to written %s", err)
	}
}

func genTimeseries(n int) *Data {
	d := &Data{}

	t := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		d.Times = append(d.Times, t)
		d.Values = append(d.Values, float64(i))
		t = t.Add(1 * time.Hour)
	}

	return d
}

func floatEqual(is, should float64) bool {
	return math.Abs(is-should) < 1e-5
}

func describe(is, should *Description) error {
	if is.First.Equal(should.First) {
		return errors.New(fmt.Sprintf("First is(%s) but should be(%s)", is.First, should.First))
	}
	if is.Last.Equal(should.Last) {
		return errors.New(fmt.Sprintf("Last is(%s) but should be(%s)", is.Last, should.Last))
	}
	if !floatEqual(is.Min, should.Min) {
		return errors.New(fmt.Sprintf("Min is(%f) but should be(%f)", is.Min, should.Min))
	}
	if !floatEqual(is.Max, should.Max) {
		return errors.New(fmt.Sprintf("Max is(%f) but should be(%f)", is.Max, should.Max))
	}
	if !floatEqual(is.Mean, should.Mean) {
		return errors.New(fmt.Sprintf("Mean is(%f) but should be(%f)", is.Mean, should.Mean))
	}
	if !floatEqual(is.Median, should.Median) {
		return errors.New(fmt.Sprintf("Median is(%f) but should be(%f)", is.Median, should.Median))
	}
	if !floatEqual(is.Std, should.Std) {
		return errors.New(fmt.Sprintf("Std is(%f) but should be(%f)", is.Std, should.Std))
	}
	if is.Count != should.Count {
		return errors.New(fmt.Sprintf("Count is(%d) but should be(%d)", is.Count, should.Count))
	}
	return nil
}

func compare(is, should *Data) error {
	nt := len(is.Times)
	mt := len(should.Times)
	if nt != mt {
		return errors.New(fmt.Sprintf("is data has %d times, should has %d times", nt, mt))
	}

	nv := len(is.Values)
	mv := len(should.Values)
	if nv != mv {
		return errors.New(fmt.Sprintf("is data has %d values, should has %d values", nv, mv))
	}

	if nt != nv {
		return errors.New(fmt.Sprintf("%d values and %d times", nv, nt))
	}

	for i := 0; i < nt; i++ {
		if !is.Times[i].Equal(should.Times[i]) {
			return errors.New(fmt.Sprintf("%d times not equal: is(%s), should(%s)",
				i, is.Times[i], should.Times[i],
			))
		}
		if !floatEqual(is.Values[i], should.Values[i]) {
			return errors.New(fmt.Sprintf("%d values not equal: is(%f), should(%f)",
				i, is.Values[i], should.Values[i],
			))
		}
	}
	return nil
}
