**Gossamr** lets you run your Go programs on Hadoop.


## Quick Example
Oh, man. Illustrating MapReduce with a word count? Get out of town.

```go
package main

import (
  "log"
  "strings"

  "github.com/vistarmedia/gossamr"
)

type WordCount struct{}

func (wc *WordCount) Map(p int64, line string, c gossamr.Collector) error {
  for _, word := range strings.Fields(line) {
    c.Collect(strings.ToLower(word), int64(1))
  }
  return nil
}

func (wc *WordCount) Reduce(word string, counts chan int64, c gossamr.Collector) error {
  var sum int64
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
```

## Running with Hadoop

    ./bin/hadoop jar ./contrib/streaming/hadoop-streaming-1.2.1.jar \
      -input /mytext.txt \
      -output /output.15 \
      -mapper "gossamr -task 0 -phase map" \
      -reducer "gossamr -task 0 -phase reduce" \
      -io typedbytes \
      -file ./wordcount
      -numReduceTasks 6
