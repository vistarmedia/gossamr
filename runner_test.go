package gossamr

import (
	"github.com/markchadwick/spec"
)

var _ = spec.Suite("Task Phase Runner", func(c *spec.C) {
	c.It("should bail with missing args", func(c *spec.C) {
		args := []string{
			"./myprog",
		}
		_, err := TaskPhaseRunnerFromArgs(args)
		c.Assert(err).NotNil()
	})

	c.It("should bail on invalid phase", func(c *spec.C) {
		args := []string{
			"./myprog",
			"-phase", "rock it",
		}
		_, err := TaskPhaseRunnerFromArgs(args)
		c.Assert(err).NotNil()
		c.Assert(err.Error()).Equals("Unknown phase rock it")
	})

	c.It("should parse task # and phase", func(c *spec.C) {
		args := []string{
			"./myprog",
			"-task", "2",
			"-phase", "combine",
		}
		r, err := TaskPhaseRunnerFromArgs(args)
		c.Assert(err).IsNil()
		c.Assert(r.taskNo).Equals(2)
		c.Assert(r.phase).Equals(CombinePhase)
	})
})

var _ = spec.Suite("Local runner", func(c *spec.C) {
	c.Skip("-pending-")
})
