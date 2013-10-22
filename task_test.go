package gossamr

import (
	"fmt"
	"github.com/markchadwick/spec"
	"io"
	"sync"
)

type Echo struct {
}

func (e *Echo) Map(k, v string, c Collector) error {
	return c.Collect(fmt.Sprintf("%s said", k), fmt.Sprintf("Hello, %s", v))
}

func (e *Echo) String() string {
	return "Echo"
}

var _ = spec.Suite("Task", func(c *spec.C) {
	echo := new(Echo)
	echoTask := NewTask(echo)
	w := NewTestBuffer()
	r := NewTestBuffer()
	defer r.Close()
	defer w.Close()

	c.It("should know when a method is missing", func(c *spec.C) {
		_, ok := echoTask.methodByName("Missing")
		c.Assert(ok).IsFalse()
	})

	c.It("should know when a method exists", func(c *spec.C) {
		mapper, ok := echoTask.methodByName("Map")
		c.Assert(ok).IsTrue()
		c.Assert(mapper).NotNil()
	})

	c.It("should not run an invalid phase", func(c *spec.C) {
		err := echoTask.Run(66, r, w)
		c.Assert(err).NotNil()
		c.Assert(err.Error()).Equals("Invalid phase 66")
	})

	c.It("should not run an unimplemented phase", func(c *spec.C) {
		err := echoTask.Run(CombinePhase, r, w)
		c.Assert(err).NotNil()
		c.Assert(err.Error()).Equals("No phase 1 for Echo")
	})

	c.It("should run a simple map phase", func(c *spec.C) {
		input := NewPairWriter(r)
		output := NewPairReader(w)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Assert(input.Write("thelma", "louise"))
			c.Assert(input.Write("abbott", "costello"))
			input.Close()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := echoTask.Run(MapPhase, r, w)
			c.Assert(err).IsNil()
		}()

		var k, v interface{}
		var err error
		k, v, err = output.Next()
		c.Assert(err).IsNil()
		c.Assert(k).Equals("thelma said")
		c.Assert(v).Equals("Hello, louise")

		k, v, err = output.Next()
		c.Assert(err).IsNil()
		c.Assert(k).Equals("abbott said")
		c.Assert(v).Equals("Hello, costello")

		k, v, err = output.Next()
		c.Assert(err).NotNil()
		c.Assert(err).Equals(io.EOF)

		wg.Wait()
	})
})
