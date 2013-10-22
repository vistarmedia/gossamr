package gossamr

import (
	"os"
)

func Run(tasks ...*Task) error {
	job := NewJob(tasks...)
	runner, err := GetRunner(os.Args)
	if err != nil {
		return err
	}
	return runner.Run(job)
}
