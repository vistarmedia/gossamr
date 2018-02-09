package gossamr

import (
	"github.com/markchadwick/spec"
	"io"
)

type TestReader struct {
	i    int
	rows [][]interface{}
}

func NewTestReader(rows [][]interface{}) *TestReader {
	return &TestReader{
		i:    0,
		rows: rows,
	}
}

func (tr *TestReader) Next() (k, v interface{}, err error) {
	if tr.i > len(tr.rows)-1 {
		return nil, nil, io.EOF
	}

	row := tr.rows[tr.i]
	tr.i++
	return row[0], row[1], nil
}

var _ = spec.Suite("Grouped Reader", func(c *spec.C) {
	c.It("should know when its input is closed", func(c *spec.C) {
		tr := &TestReader{}
		gr := NewGroupedReader(tr)

		_, _, err := gr.Next()
		c.Assert(err).Equals(io.EOF)
	})

	c.It("should group adjacent keys", func(c *spec.C) {
		tr := NewTestReader([][]interface{}{
			{"seen", 12},
			{"seen", 82},
		})
		gr := NewGroupedReader(tr)

		key, vs, err := gr.Next()
		c.Assert(err).IsNil()
		c.Assert(key).Equals("seen")

		ch, ok := vs.(chan int)
		c.Assert(ok).IsTrue()

		observed := make([]int, 0)
		for o := range ch {
			observed = append(observed, o)
		}
		c.Assert(observed).HasLen(2)
		c.Assert(observed[0]).Equals(12)
		c.Assert(observed[1]).Equals(82)

		key, vs, err = gr.Next()
		c.Assert(err).Equals(io.EOF)
		c.Assert(key).IsNil()
		c.Assert(vs).IsNil()
	})

	c.It("should aggregate all similar keys", func(c *spec.C) {
		tr := NewTestReader([][]interface{}{
			{"delivered", 10},
			{"delivered", 80},
			{"seen", 12},
			{"seen", 82},
		})
		gr := NewGroupedReader(tr)

		key, vs, err := gr.Next()
		c.Assert(err).IsNil()
		c.Assert(key).Equals("delivered")

		ch, ok := vs.(chan int)
		c.Assert(ok).IsTrue()

		observed := make([]int, 0)
		for o := range ch {
			observed = append(observed, o)
		}
		c.Assert(observed).HasLen(2)
		c.Assert(observed[0]).Equals(10)
		c.Assert(observed[1]).Equals(80)

		key, vs, err = gr.Next()
		c.Assert(err).IsNil()
		c.Assert(key).Equals("seen")

		ch, ok = vs.(chan int)
		c.Assert(ok).IsTrue()

		observed = make([]int, 0)
		for o := range ch {
			observed = append(observed, o)
		}
		c.Assert(observed).HasLen(2)
		c.Assert(observed[0]).Equals(12)
		c.Assert(observed[1]).Equals(82)

		key, vs, err = gr.Next()
		c.Assert(err).Equals(io.EOF)
		c.Assert(key).IsNil()
		c.Assert(vs).IsNil()
	})
})
