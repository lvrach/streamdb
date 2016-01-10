package store

import (
	"fmt"
	"github.com/boltdb/bolt"
)

type stream struct {
	db      *bolt.DB
	bucket  []byte
	observers []Observer
}

func (s *stream) Push(rows []Row) error {

	//FIXME
	row := rows[0]

    err := s.db.Update(func(tx *bolt.Tx) error {
        // Retrieve the users bucket.
        // This should be created when the DB is first opened.
        b := tx.Bucket(s.bucket)

        row.id, _ = b.NextSequence()


        key, value, err := row.EncodeBolt()
        if (err != nil) {
        	return err
        }

        fmt.Println("write", key, row.data);

        return b.Put(key, value)
    })

    if err != nil {
    	return err
    }

	for _, observ := range s.observers {
		select{
	    	case observ <- rows:
	    	default: continue
	    }
	}

	return nil
}

type Observer chan []Row

type Cursor struct { 
	ch Observer
	pstream *stream
	closed bool
}

type Iterator interface {
	Data(row Row)
	Flush()
}

func (s *stream) Iteratorate(iter Iterator) (*Cursor) {
	ch := make(chan []Row, 1000)
	c := &Cursor{ch, s, false}

	s.db.View(func(tx *bolt.Tx) error {
	    b := tx.Bucket(s.bucket)
	    bc := b.Cursor()

	    for k, v := bc.First(); k != nil; k, v = bc.Next() {
	    	row := Row{}
	    	row.DecodeBolt(k, v)
	    	
	    	if c.closed == true {
   			 	fmt.Println("Stop on going query on",s.bucket)
   			 	return nil
	    	}

	    	iter.Data(row)
	    }

	    iter.Flush()

	    return nil
	})
	s.observers = append(s.observers, c.ch)

	go func () {
		for rows := range c.ch {
			for _, row := range rows {
				iter.Data(row)
			}
			iter.Flush()
		}
	}()

	return c;
}

func (c Cursor) Close() error {

	s := c.pstream

	c.closed = true

	for i, ch := range s.observers {
		if ch == c.ch {
			s.observers[i] = s.observers[len(s.observers)-1]
			s.observers = s.observers[:len(s.observers)-1]
		}
	}

	c.ch = nil
	c.pstream = nil

	return nil
}