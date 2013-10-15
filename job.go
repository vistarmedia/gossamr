package main

type Collector interface {
	Collect(k, v interface{}) error
}

type writerCollector struct {
	writer Writer
}

func NewWriterCollector(writer Writer) *writerCollector {
	return &writerCollector{writer}
}

func (wc *writerCollector) Collect(k, v interface{}) error {
	return wc.writer.Write(k, v)
}

type Job struct {
	reader Reader
	writer Writer
	tasks  []*Task
}

func NewJob(r Reader, w Writer, tasks ...*Task) *Job {
	return &Job{
		reader: r,
		writer: w,
		tasks:  tasks,
	}
}
