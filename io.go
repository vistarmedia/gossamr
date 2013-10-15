package main

import (
	"bufio"
	"fmt"
	"github.com/markchadwick/typedbytes"
	"io"
)

type Reader interface {
	Next() (k, v interface{}, err error)
}

type Writer interface {
	Write(k, v interface{}) error
	Close() error
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
