package tsm

import (
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

func (bs *boltStore) WriteData(id, domain string, data *Data) error {
	return nil
}
func (bs *boltStore) ReadData(id, domain string, start, end time.Time) (*Data, error) {
	return &Data{}, nil
}
func (bs *boltStore) Describe(id, domain string) (*Description, error) {
	return &Description{}, nil
}

func (bs *boltStore) Description() string {
	return fmt.Sprintf(desc, bs.threshold, bs.path)
}
