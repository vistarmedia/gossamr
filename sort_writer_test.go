package gossamr

import (
	"fmt"
	"github.com/markchadwick/spec"
	"log"
	"math/rand"
)

var _ = spec.Suite("Sort Writer", func(c *spec.C) {
	c.It("should flipping run", func(c *spec.C) {
		buf := NewBufCloser()
		sw, err := NewSortWriter(buf, 10)
		c.Assert(err).IsNil()

		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("rec-%05d", rand.Int31n(100))
			c.Assert(sw.Write(key, int32(i))).IsNil()
		}
		c.Assert(sw.Close()).IsNil()

		pr := NewPairReader(buf)
		for {
			k, v, err := pr.Next()
			if err != nil {
				return
			}
			log.Printf("%v = %v", k, v)
		}
	})

})
