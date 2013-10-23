package gossamr

import (
	"github.com/markchadwick/sortedpairs"
	"github.com/markchadwick/typedbytes"
	"io"
)

type pairWriter struct {
	w io.Writer
}

func (pw *pairWriter) Write(k, v []byte) (err error) {
	if _, err = pw.w.Write(k); err != nil {
		return
	}
	_, err = pw.w.Write(v)
	return
}

type SortWriter struct {
	w   io.WriteCloser
	spw *sortedpairs.Writer
}

func NewSortWriter(w io.WriteCloser, capacity int) (*SortWriter, error) {
	pw := &pairWriter{w}
	spw, err := sortedpairs.NewWriter(pw, capacity)
	if err != nil {
		return nil, err
	}
	sw := &SortWriter{
		w:   w,
		spw: spw,
	}
	return sw, nil
}

func (sw *SortWriter) Write(k, v interface{}) (err error) {
	var kb, vb []byte
	if kb, err = typedbytes.Encode(k); err != nil {
		return
	}
	if vb, err = typedbytes.Encode(v); err != nil {
		return
	}
	return sw.spw.Write(kb, vb)
}

func (sw *SortWriter) Close() (err error) {
	err = sw.spw.Close()
	sw.w.Close()
	return
}
