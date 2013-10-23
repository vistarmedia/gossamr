package gossamr

import (
	"bufio"
	"fmt"
	"github.com/markchadwick/typedbytes"
	"io"
	"log"
	"reflect"
)

func Copy(r Reader, w Writer) (err error) {
	var k, v interface{}
	for {
		if k, v, err = r.Next(); err != nil {
			if err == io.EOF {
				return nil
			}
			return
		}
		if err = w.Write(k, v); err != nil {
			return
		}
	}
	return nil
}

type Reader interface {
	Next() (k, v interface{}, err error)
}

type Writer interface {
	Write(k, v interface{}) error
	Close() error
}

// A reader that, for each key, will group all its values into a channel.
type GroupedReader struct {
	nextKey   interface{}
	nextValue interface{}
	nextError error
	reader    Reader
}

func NewGroupedReader(reader Reader) Reader {
	return &GroupedReader{
		nextKey:   nil,
		nextValue: nil,
		reader:    reader,
	}
}

func (gr *GroupedReader) Next() (k, v interface{}, err error) {
	log.Printf("GroupedReader.next")
	if gr.nextError != nil {
		log.Printf("bail1: %v", err)
		err = gr.nextError
		return
	}

	if gr.nextKey == nil && gr.nextValue == nil {
		gr.nextKey, gr.nextValue, err = gr.reader.Next()
		if err != nil {
			log.Printf("bail2: %v", err)
			return
		}
		log.Printf("nk: %v, nv: %v", gr.nextKey, gr.nextValue)
	}

	key := gr.nextKey
	t := reflect.ChanOf(reflect.BothDir, reflect.TypeOf(gr.nextValue))
	ch := reflect.MakeChan(t, 0)

	go func() {
		defer ch.Close()
		ch.Send(reflect.ValueOf(gr.nextValue))
		for {
			k, v, err = gr.reader.Next()
			if err != nil {
				gr.nextError = err
				return
			}
			if k != key {
				gr.nextKey = k
				gr.nextValue = v
				return
			}
			ch.Send(reflect.ValueOf(v))
		}
	}()
	return key, ch.Interface(), nil
}

// Read pairs serialized with Hadoop's typedbytes. It is assumed that in
// non-local mode, this will always be the wire format for reading and writing.
func NewPairReader(r io.Reader) Reader {
	byteReader := typedbytes.NewReader(r)
	return typedbytes.NewPairReader(byteReader)
}

// Write pairs to an underlying writer in Hadoop's typedbytes format. As above,
// it is assumed all non-local IO will happen in this format
func NewPairWriter(w io.WriteCloser) Writer {
	byteWriter := typedbytes.NewWriter(w)
	return typedbytes.NewPairWriter(byteWriter)
}

// Line Reader is used by basic streaming jobs. It yeilds a line number and the
// raw line delimited by \n. The consumer must accept the arguments (int64,
// string).
type LineReader struct {
	n      int64
	reader *bufio.Reader
}

func NewLineReader(r io.Reader) *LineReader {
	reader := bufio.NewReader(r)
	return &LineReader{
		n:      0,
		reader: reader,
	}
}

func (lr *LineReader) Next() (k, v interface{}, err error) {
	k = lr.n
	var line []byte
	line, _, err = lr.reader.ReadLine()
	lr.n += int64(len(line))
	v = string(line)
	return
}

// StringWriter will coax each key/value to a simple string and output it in
// simple streaming format: key\tvalue\n
type StringWriter struct {
	w io.WriteCloser
}

func NewStringWriter(w io.WriteCloser) *StringWriter {
	return &StringWriter{w}
}

func (sw *StringWriter) Write(k, v interface{}) (err error) {
	_, err = fmt.Fprintf(sw.w, "%v\t%v\n", k, v)
	return
}

func (sw *StringWriter) Close() error {
	return sw.w.Close()
}
