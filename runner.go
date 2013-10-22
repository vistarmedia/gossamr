// Runs a job (or part of a job). There are three primary types of runners
//
//  1. LocalRunner - Used for simulating a job locally. The sorting and
//  combining functions of Hadoop will be emulated as best as possible, though
//  no guarentees are made
//  2. TaskPhaseRunner - Used inter-step during a Hadoop job. This runs a single
//  phase of a task
//  3. JobRunner - Submits a multi-task Job to hadoop, organizing temporary
//  files and forking the necessary processes.
package gossamr

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
)

// Given the arguments, figure out which runner should be used.
func GetRunner(args []string) (Runner, error) {
	if argsContain(args, "-task") {
		return TaskPhaseRunnerFromArgs(args)
	}

	return new(LocalRunner), nil
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

// LocalRunner
type LocalRunner struct {
	root string
}

func (lr *LocalRunner) Run(j *Job) (err error) {
	if lr.root, err = ioutil.TempDir("", "gossamr-"); err != nil {
		return
	}
	log.Printf("Working in %s", lr.root)
	defer os.RemoveAll(lr.root)
	return lr.runJob(j)
}

func (lr *LocalRunner) runJob(j *Job) (err error) {
	input := NewLineReader(os.Stdin)
	var fname string
	var output *os.File
	for i, task := range j.tasks {
		if fname, err = lr.runTask(i, task, input); err != nil {
			return
		}
	}
	if fname == "" {
		return nil
	}

	if output, err = os.Open(fname); err != nil {
		return
	}
	reader := NewPairReader(output)
	writer := NewStringWriter(os.Stdout)
	return Copy(reader, writer)
}

func (lr *LocalRunner) runTask(i int, task *Task, in Reader) (name string, err error) {
	mapper, ok := task.mapper()
	if !ok {
		return "", fmt.Errorf("Task[%d] has no mapper", i)
	}
	var output *os.File
	output, err = lr.open(i, "map-output")
	if err != nil {
		return "", err
	}

	writer, err := NewSortWriter(output, 1024*1024)
	if err != nil {
		return "", err
	}
	err = task.run(mapper, in, writer)
	if err != nil {
		return "", err
	}
	return output.Name(), err
}

func (lr *LocalRunner) open(i int, name string) (f *os.File, err error) {
	fname := path.Join(lr.root, fmt.Sprintf("%03d-%s", i, name))
	return os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644)
}

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
