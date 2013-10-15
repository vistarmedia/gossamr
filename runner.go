// Runs a job (or part of a job). There are three primary types of runners
//
//  1. LocalRunner - Used for simulating a job locally. The sorting and
//  combining functions of Hadoop will be emulated as best as possible, though
//  no guarentees are made
//  2. TaskPhaseRunner - Used inter-step during a Hadoop job. This runs a single
//  phase of a task
//  3. JobRunner - Submits a multi-task Job to hadoop, organizing temporary
//  files and forking the necessary processes.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

// Given the arguments, figure out which runner should be used.
func GetRunner(args []string) (Runner, error) {
	if argsContain(args, "-task") {
		return TaskPhaseRunnerFromArgs(args)
	}
	return nil, nil
}

func argsContain(args []string, s string) bool {
	for _, arg := range args {
		if arg == s {
			return true
		}
	}
	return false
}

type Runner interface {
	Run(job *Job) error
}

// ----------------------------------------------------------------------------
// LocalRunner -- pending
// ----------------------------------------------------------------------------

// TaskPhaseRunner
// Runs a single phase of a task forked from Hadoop. It is assumed that all
// input and output will be typed bytes at this point.
type TaskPhaseRunner struct {
	taskNo int
	phase  Phase
}

func TaskPhaseRunnerFromArgs(args []string) (tpr *TaskPhaseRunner, err error) {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	taskNo := fs.Int("task", 0, "task # to run")
	phaseName := fs.String("phase", "", "phase of task to run")

	if err = fs.Parse(args[1:]); err != nil {
		return
	}

	phase, err := GetPhase(*phaseName)
	if err != nil {
		return nil, err
	}

	tpr = &TaskPhaseRunner{
		taskNo: *taskNo,
		phase:  phase,
	}
	return
}

func (tpr *TaskPhaseRunner) Run(j *Job) error {
	if tpr.taskNo > len(j.tasks)-1 {
		return fmt.Errorf("No task %d", tpr.taskNo)
	}
	task := j.tasks[tpr.taskNo]
	log.Printf("Running phase %d with TaskPhaseRunner", tpr.phase)
	return task.Run(tpr.phase, os.Stdin, os.Stdout)
}
