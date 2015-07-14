package main

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"time"

	"github.com/espang/tsm"
)

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func BenchWrite(s tsm.Store, n, m int) (time.Duration, time.Duration, time.Duration, time.Duration) {
	d := &tsm.Data{}
	t := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < m; i++ {
		d.Times = append(d.Times, t)
		d.Values = append(d.Values, float64(i))
		t = t.Add(1 * time.Hour)
	}

	durs := make([]int64, 0, n)
	errs := make([]error, 0)
	var total int64
	var err error
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("id_%d", i)
		start := time.Now()
		s.WriteData(id, "domain", d)
		dur := int64(time.Since(start))
		durs = append(durs, dur)
		total += dur
		if err != nil {
			errs = append(errs, err)
		}
	}
	sort.Sort(Int64Slice(durs))
	var median time.Duration
	if n%2 == 0 {
		median = time.Duration((durs[(n-1)/2] + durs[n/2]) / 2)
	} else {
		median = time.Duration(durs[(n-1)/2])
	}
	min := time.Duration(durs[0])
	max := time.Duration(durs[n-1])
	mean := time.Duration(total / int64(n))
	return min, max, mean, median
}

func main() {
	var store tsm.Store
	var err error
	store, err = tsm.NewBoltStore("bench.db")
	if err != nil {
		log.Fatalf("Error getting store: %s", err)
	}
	fmt.Printf("Connected to %v\n", store)
	var min, max, mean, median time.Duration
	fmt.Printf("%10s|%10s|%10s|%10s|%10s|%10s\n",
		"CPUS", "Desc", "Min", "Max", "Mean", "Median",
	)
	for ncpus := 1; ncpus <= runtime.NumCPU()*2; ncpus++ {
		runtime.GOMAXPROCS(ncpus)
		min, max, mean, median = BenchWrite(store, 101, 1000)
		fmt.Printf("%10d|%10s|%10s|%10s|%10s|%10s\n",
			ncpus,
			"101x1000",
			min,
			max,
			mean,
			median,
		)
	}
}
