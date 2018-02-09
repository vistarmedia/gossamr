// Runs a job (or part of a job). There are three primary types of runners
//
//  1. LocalRunner - Used for simulating a job locally. The sorting and
//  combining functions of Hadoop will be emulated as best as possible, though
//  no guarantees are made
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
	"reflect"
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

func (lr *LocalRunner) runTask(i int, t *Task, in Reader) (output string, err error) {
	var f *os.File
	mapper, hasMapper := t.mapper()
	combiner, hasCombiner := t.combiner()
	reducer, hasReducer := t.reducer()

	// A task must have a mapper
	if !hasMapper {
		return "", fmt.Errorf("Task[%d] has no mapper", i)
	}
	mapOutput, err := lr.open(i, "mapper")
	if err != nil {
		return "", err
	}
	output = mapOutput.Name()

	if hasCombiner || hasReducer {
		if err = lr.execSorted(t, mapper, in, mapOutput); err != nil {
			return
		}
	} else {
		if err = lr.exec(t, mapper, in, mapOutput); err != nil {
			return
		}
	}

	if hasCombiner {
		if f, err = os.Open(output); err != nil {
			return output, err
		}
		in = NewGroupedReader(NewPairReader(f))

		combineOutput, err := lr.open(i, "combiner")
		if err != nil {
			return "", err
		}
		output = combineOutput.Name()

		if err = lr.execSorted(t, combiner, in, combineOutput); err != nil {
			return output, err
		}
	}

	if hasReducer {
		if f, err = os.Open(output); err != nil {
			return output, err
		}
		in = NewGroupedReader(NewPairReader(f))

		reduceOutput, err := lr.open(i, "reducer")
		if err != nil {
			return "", err
		}
		output = reduceOutput.Name()
		if err = lr.execSorted(t, reducer, in, reduceOutput); err != nil {
			return output, err
		}
	}

	return
}

func (lr *LocalRunner) execSorted(t *Task, f reflect.Value, r Reader, out *os.File) error {
	w, err := NewSortWriter(out, 1024*1024)
	if err != nil {
		return err
	}
	return t.run(f, r, w)
}

func (lr *LocalRunner) exec(t *Task, f reflect.Value, r Reader, out *os.File) error {
	w := NewPairWriter(out)
	return t.run(f, r, w)
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
