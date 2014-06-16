package main

import (
	"log"
	"strings"

	// "github.com/vistarmedia/gossamr"
	gossamr "../../"
)

type WordCount struct{}

func (wc *WordCount) Map(p int64, line string, c gossamr.Collector) error {
	for _, word := range strings.Fields(line) {
		c.Collect(strings.ToLower(word), int64(1))
	}
	return nil
}

func (wc *WordCount) Reduce(word string, counts chan int64, c gossamr.Collector) error {
	var sum int64 = 0
	for v := range counts {
		sum += v
	}
	c.Collect(sum, word)
	return nil
}

func main() {
	wordcount := gossamr.NewTask(&WordCount{})

	err := gossamr.Run(wordcount)
	if err != nil {
		log.Fatal(err)
	}
}
