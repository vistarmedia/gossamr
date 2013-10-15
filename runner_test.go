package main

import (
	"github.com/markchadwick/spec"
)

var _ = spec.Suite("Task Phase Runner", func(c *spec.C) {

	c.It("should bail with missing args", func(c *spec.C) {
		args := []string{
			"./myprog",
			"-h",
		}
		_, err := TaskPhaseRunnerFromArgs(args)
		c.Assert(err).NotNil()
		c.Assert(err.Error()).Equals("Missing phase")
	})

	c.It("should bail on invalid phase", func(c *spec.C) {
		args := []string{
			"./myprog",
			"-phase", "rock it",
		}
		_, err := TaskPhaseRunnerFromArgs(args)
		c.Assert(err).NotNil()
		c.Assert(err.Error()).Equals("Missing phase")
	})
})
