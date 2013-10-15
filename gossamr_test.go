package main

import (
	"bufio"
	"github.com/markchadwick/spec"
	"io"
	"testing"
)

func Test(t *testing.T) {
	spec.Run(t)
}

// An in-memory buffer to block while input is pending and it is not closed.
type TestBuffer struct {
	r  *io.PipeReader
	w  *io.PipeWriter
	br *bufio.Reader
	bw *bufio.Writer
}

func NewTestBuffer() *TestBuffer {
	r, w := io.Pipe()
	return &TestBuffer{
		r:  r,
		w:  w,
		br: bufio.NewReader(r),
		bw: bufio.NewWriter(w),
	}
}

func (tb *TestBuffer) Read(p []byte) (int, error) {
	return tb.br.Read(p)
}

func (tb *TestBuffer) Write(p []byte) (int, error) {
	return tb.bw.Write(p)
}

func (tb *TestBuffer) Close() (err error) {
	if err = tb.bw.Flush(); err != nil {
		return
	}
	return tb.w.CloseWithError(io.EOF)
}
