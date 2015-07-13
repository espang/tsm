package tsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

var desc = "Bolt database opened at '%s' in path '%s'"

type boltStore struct {
	db        *bolt.DB
	path      string
	threshold time.Time
}

// NewBoltStore returns a new, initialized BoltStore.
func NewBoltStore(path string) (bs *boltStore, err error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	bs = &boltStore{db: db, threshold: time.Now(), path: path}
	return bs, nil
}

func timeToBytes(t time.Time) ([]byte, error) {
	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, t.UnixNano())
	if err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}

func bytesToTime(buf []byte) (time.Time, error) {
	r := bytes.NewReader(buf)
	var nsec int64
	err := binary.Read(r, binary.BigEndian, &nsec)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(nsec/1e9, nsec%1e9).UTC(), nil
}

func float64ToBytes(f float64) ([]byte, error) {
	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.LittleEndian, f)
	if err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}
func bytesToFloat64(buf []byte) (float64, error) {
	r := bytes.NewReader(buf)
	var f float64
	err := binary.Read(r, binary.LittleEndian, &f)
	if err != nil {
		return 0., err
	}
	return f, nil
}

// WriteData updates the timeseries "id" in the given "domain"
//
// At first all values with times in the range from the first
// till the last time.Time in the Times slice will be deleted
//
// All the values will be written with time.Time keys and float64
// values.
func (bs *boltStore) WriteData(id, domain string, data *Data) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		domainBucket, err := tx.CreateBucketIfNotExists([]byte(domain))
		if err != nil {
			return err
		}
		tsBucket, err := domainBucket.CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return err
		}

		c := tsBucket.Cursor()
		startBytes, err := timeToBytes(data.Times[0])
		if err != nil {
			return err
		}
		endBytes, err := timeToBytes(data.Times[len(data.Times)-1])
		if err != nil {
			return err
		}
		for k, _ := c.Seek(startBytes); k != nil && bytes.Compare(k, endBytes) <= 0; k, _ = c.Next() {
			err = c.Delete()
			if err != nil {
				return err
			}
		}
		if len(data.Times) != len(data.Values) {
			return errors.New("Times, Values length mismatch")
		}

		for i := 0; i < len(data.Times); i++ {
			k, err := timeToBytes(data.Times[i])
			if err != nil {
				return err
			}
			v, err := float64ToBytes(data.Values[i])
			if err != nil {
				return err
			}
			tsBucket.Put(k, v)
		}
		return nil
	})
}
func (bs *boltStore) ReadData(id, domain string, start, end time.Time) (*Data, error) {
	times := make([]time.Time, 0)
	values := make([]float64, 0)
	err := bs.db.View(func(tx *bolt.Tx) error {
		domainBucket := tx.Bucket([]byte(domain))
		if domainBucket == nil {
			return errors.New(fmt.Sprintf("No domain '%s'", domain))
		}
		tsBucket := domainBucket.Bucket([]byte(id))
		if tsBucket == nil {
			return errors.New(fmt.Sprintf("No time series with id '%s' in domain '%s'", id, domain))
		}
		c := tsBucket.Cursor()
		startBytes, err := timeToBytes(start)
		if err != nil {
			return err
		}
		endBytes, err := timeToBytes(end)
		if err != nil {
			return err
		}
		var f float64
		for k, v := c.Seek(startBytes); k != nil && bytes.Compare(k, endBytes) <= 0; k, v = c.Next() {
			t, err := bytesToTime(k)
			if err != nil {
				return err
			}
			f, err = bytesToFloat64(v)
			if err != nil {
				return err
			}
			times = append(times, t)
			values = append(values, f)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Data{Times: times, Values: values}, nil
}
func (bs *boltStore) Describe(id, domain string) (*Description, error) {

	err := bs.db.View(func(tx *bolt.Tx) error {
		domainBucket := tx.Bucket([]byte(domain))
		if domainBucket == nil {
			return errors.New(fmt.Sprintf("No domain '%s'", domain))
		}
		tsBucket := domainBucket.Bucket([]byte(id))
		if tsBucket == nil {
			return errors.New(fmt.Sprintf("No time series with id '%s' in domain '%s'", id, domain))
		}
		c := tsBucket.Cursor()
		startBytes, err := timeToBytes(start)
		if err != nil {
			return err
		}
		endBytes, err := timeToBytes(end)
		if err != nil {
			return err
		}
		var f float64
		for k, v := c.First(); k != nil; k, v = c.Next() {
			t, err := bytesToTime(k)
			if err != nil {
				return err
			}
			f, err = bytesToFloat64(v)
			if err != nil {
				return err
			}
			times = append(times, t)
			values = append(values, f)
		}
		return nil
	})
	return &Description{}, nil
}

func (bs *boltStore) Description() string {
	return fmt.Sprintf(desc, bs.threshold, bs.path)
}
