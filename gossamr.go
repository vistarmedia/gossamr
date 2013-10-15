package main

import (
	"log"
	"regexp"
	"strings"
)

func Run(tasks ...*Task) error {
	// reader := NewLineReader(os.Stdin)
	// writer := NewStringWriter(os.Stdout)
	// reader := NewPairReader(os.Stdin)
	// writer := NewPairWriter(os.Stdout)
	// job := NewJob(reader, writer, tasks...)
	// runner := new(LocalRunner)
	// return runner.Run(job)
	return nil
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
	// c.Collect(n, line)
	for _, word := range wc.re.FindAllString(line, -1) {
		c.Collect(strings.ToLower(word), 1)
	}
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
	// reader := NewLineReader(os.Stdin)
	// writer := NewPairWriter(os.Stdout)
	// defer writer.Close()

	// for {
	//   b, line, err := reader.Next()
	//   if err != nil {
	//     return
	//   }
	//   writer.Write(b, line)
	// }
}
