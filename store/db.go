package store

import (
	"log"
	"github.com/boltdb/bolt"
	"time"
	"fmt"
)


type DB struct {
	name string
	bolt *bolt.DB
	streams map[string]*stream
}

func Open(name string) (*DB) {
    db, err := bolt.Open("default.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        log.Fatal(err)
    }

    streams := make(map[string]*stream)

    return &DB{
    	name, 
    	db, 
    	streams,
    }
}

func (db *DB) Close() {
	db.bolt.Close();
}

func (db *DB) Stream(streamId string) *stream {
	_, ok := db.streams[streamId]

	if ok {
		return db.streams[streamId]
	}

	log.Println("Creating new stream", streamId)
	if db.streams == nil {
		db.streams = make(map[string]*stream)
	}

	bucket := []byte(streamId)

	db.bolt.Update(func(tx *bolt.Tx) error {
	    _, err := tx.CreateBucket(bucket)

	    if err != nil {
	        return fmt.Errorf("create bucket: %s", err)
	    }
	    
	    return nil
	});

	db.streams[streamId] = &stream{
		db.bolt,
		bucket,
		make([]Observer,0),
	}
	
	return db.streams[streamId]
}