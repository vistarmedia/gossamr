package main

import (
	"log"
)

type Collector interface {
	Collect(k, v interface{}) error
}

type writerCollector struct {
	writer Writer
}

var _ Collector = new(writerCollector)

func NewWriterCollector(writer Writer) *writerCollector {
	return &writerCollector{writer}
}

func (wc *writerCollector) Collect(k, v interface{}) (err error) {
	err = wc.writer.Write(k, v)
	if err != nil {
		log.Printf("error writing to collector: %s", err.Error())
	}
	return
}

type Job struct {
	// reader Reader
	// writer Writer
	tasks []*Task
}

func NewJob(tasks ...*Task) *Job {
	return &Job{
		// reader: r,
		// writer: w,
		tasks: tasks,
	}
}
