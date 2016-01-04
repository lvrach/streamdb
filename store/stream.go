package store

import (
	"fmt"
	"log"
	"math"
)

type Row map[string]interface{}

func (row Row) Number(field string) (float64, error) {

	value, ok := row[field]
	if !ok {
		return math.NaN(), fmt.Errorf("field %s not defined", field)
	}

	switch i := value.(type) {
	case float64:
		return i, nil
	default:
		return math.NaN(), fmt.Errorf("%v %v not an number", i, value)
	}
}

type DB struct {
	streams map[string]*stream
}

type stream struct {
	list    []Row
	observers []Observer
}

func (db *DB) Stream(streamId string) *stream {
	_, ok := db.streams[streamId]

	if !ok {
		log.Println("Creating new stream", streamId)
		if db.streams == nil {
			db.streams = make(map[string]*stream)
		}
		db.streams[streamId] = new(stream)
	}
	return db.streams[streamId]
}

func (s *stream) Push(data []Row) {
	s.list = append(s.list, data...)

	for _, observ := range s.observers {
		select{
	    	case observ <- data:
	    	default: continue
	    }
	}
}

type Observer chan []Row

type Cursor struct { 
	ch Observer
	pstream *stream
}


func (s *stream) Cursor() (Cursor) {
	ch := make(chan []Row, 100)
	ch <- s.list
	s.observers = append(s.observers, ch)

	return Cursor{ch, s}
}
func (c Cursor) Data() (Observer) {
	return c.ch;
}

func (c Cursor) Close() error {

	s := c.pstream

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