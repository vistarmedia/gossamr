package main

import (
	"log"
	"os"
	"regexp"
	"strings"
)

func init() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
}

func Run(tasks ...*Task) error {
	job := NewJob(tasks...)
	runner, err := GetRunner(os.Args)
	if err != nil {
		return err
	}
	return runner.Run(job)
}

// ----------------------------------------------------------------------------
// Example
// ----------------------------------------------------------------------------

type WordCount struct {
	re *regexp.Regexp
}

func NewWordCount() (wc *WordCount, err error) {
	wc = new(WordCount)
	wc.re, err = regexp.Compile("[a-zA-Z0-9]+")
	return
}

func (wc *WordCount) Map(n int64, line string, c Collector) {
	for _, word := range wc.re.FindAllString(line, -1) {
		c.Collect(strings.ToLower(word), int32(1))
	}
}

func (wc *WordCount) Combine(word string, counts chan int32, c Collector) {
	var sum int32
	for count := range counts {
		sum += count
	}
	c.Collect(word, sum)
}

func (wc *WordCount) Reduce(word string, counts chan int32, c Collector) {
	var sum int32
	for count := range counts {
		sum += count
	}
	c.Collect(word, sum)
}

func runWordCount() error {
	wc, err := NewWordCount()
	if err != nil {
		log.Fatal(err)
	}
	task := NewTask(wc)
	return Run(task)
}

func main() {
	if err := runWordCount(); err != nil {
		log.Fatal(err)
	}
}
