package store

import (
	"fmt"
	"encoding/json"
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"math"
)

type Row struct {
	id uint64
	data map[string]interface{}
}

func init() {
	gob.Register(map[string]interface{}{})
}
func (row Row) Number(field string) (float64, error) {

	value, ok := row.data[field]
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

func (row *Row) EncodeBolt() (key, value []byte, _ error) {
    
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(&row.data)
    if err != nil {
        return nil, nil, err
    }
    value = buf.Bytes()

   	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, row.id)
    if err != nil {
        return nil, nil, err
    }
    key = buf.Bytes() 

    return key, value, nil
}

func (row *Row) DecodeBolt(key []byte, value []byte) error {
    
    buf := bytes.NewReader(value)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&row.data)
    if err != nil {
        return err
    }

   	buf = bytes.NewReader(key)
	err = binary.Read(buf, binary.LittleEndian, &row.id)
	if err != nil {
		return fmt.Errorf("fail to parse key: %s", err)
	}

    return nil
}

func (row *Row) UnmarshalJSON(data []byte) error {
    err := json.Unmarshal(data, &row.data)
    if err != nil {
        return err
    }

    return nil
}