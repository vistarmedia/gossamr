package main

import (
	"fmt"
	"io"
	"reflect"
)

type Phase uint8

const (
	MapPhase Phase = iota
	CombinePhase
	ReducePhase
)

func GetPhase(name string) (Phase, error) {
	switch name {
	default:
		return 0, fmt.Errorf("Unknown phase %s", name)
	case "":
		return 0, fmt.Errorf("Missing phase")
	case "map":
		return MapPhase, nil
	case "combine":
		return CombinePhase, nil
	case "reduce":
		return ReducePhase, nil
	}
}

type Task struct {
	instance interface{}
	value    reflect.Value
}

func NewTask(instance interface{}) *Task {
	value := reflect.ValueOf(instance)
	return &Task{
		instance: instance,
		value:    value,
	}
}

func (t *Task) Run(phase Phase, r io.Reader, w io.WriteCloser) error {
	var m reflect.Value
	var ok bool
	switch phase {
	default:
		return fmt.Errorf("Invalid phase %d", phase)
	case MapPhase:
		m, ok = t.mapper()
	case CombinePhase:
		m, ok = t.combiner()
	case ReducePhase:
		m, ok = t.reducer()
	}
	if !ok {
		return fmt.Errorf("No phase %d for %s", phase, t.instance)
	}
	return t.run(m, r, w)
}

func (t *Task) run(m reflect.Value, r io.Reader, w io.WriteCloser) (err error) {
	input := NewPairReader(r)
	output := NewPairWriter(w)
	collector := NewWriterCollector(output)
	colValue := reflect.ValueOf(collector)

	defer func() {
		if e := output.Close(); e != nil && err == nil {
			err = e
		}
	}()

	var k, v interface{}
	for {
		k, v, err = input.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return
		}
		m.Call([]reflect.Value{
			reflect.ValueOf(k),
			reflect.ValueOf(v),
			colValue,
		})
	}

	return
}

func (t *Task) mapper() (reflect.Value, bool) {
	return t.methodByName("Map")
}

func (t *Task) combiner() (reflect.Value, bool) {
	return t.methodByName("Combine")
}

func (t *Task) reducer() (reflect.Value, bool) {
	return t.methodByName("Reduce")
}

func (t *Task) methodByName(name string) (v reflect.Value, ok bool) {
	v = t.value.MethodByName(name)
	ok = v.Kind() == reflect.Func
	return
}
