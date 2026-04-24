// Package mix runs several workloads concurrently with configurable weights,
// implementing PRD §§5.7–5.8 (write-intensive and read-intensive mixes). Each
// nested workload keeps its own thread pool, weighted so that
// sum(threads * weight) == env.Threads.
package mix

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

// TypeName is the YAML type: value.
const TypeName = "mix"

// Register wires the mix factory. The caller must have already registered the
// nested workload types before Build() is invoked.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		raw, ok := w.Params["workloads"]
		if !ok {
			return nil, fmt.Errorf("mix %q: missing nested workloads", w.Name)
		}
		nestedList, ok := raw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("mix %q: workloads must be a list", w.Name)
		}
		m := &Workload{name: w.Name}
		for i, item := range nestedList {
			asMap, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("mix %q: nested[%d] must be a map", w.Name, i)
			}
			nw, err := parseNested(asMap)
			if err != nil {
				return nil, fmt.Errorf("mix %q nested[%d]: %w", w.Name, i, err)
			}
			child, err := workload.Build(nw.plan)
			if err != nil {
				return nil, fmt.Errorf("mix %q: child %q: %w", w.Name, nw.plan.Name, err)
			}
			m.children = append(m.children, child)
			m.weights = append(m.weights, nw.weight)
		}
		if len(m.children) == 0 {
			return nil, fmt.Errorf("mix %q: no nested workloads defined", w.Name)
		}
		return m, nil
	})
}

type nested struct {
	plan   plan.Workload
	weight int
}

func parseNested(m map[string]interface{}) (nested, error) {
	n := nested{weight: 1}
	name, _ := m["name"].(string)
	typeName, _ := m["type"].(string)
	if name == "" || typeName == "" {
		return n, errors.New("nested workload requires name and type")
	}
	n.plan = plan.Workload{Name: name, Type: typeName}
	if w, ok := m["weight"].(float64); ok {
		n.weight = int(w)
	} else if w, ok := m["weight"].(int); ok {
		n.weight = w
	}
	if n.weight <= 0 {
		n.weight = 1
	}
	if sz, ok := m["object_size"].(float64); ok {
		n.plan.ObjectSize = plan.Size(int64(sz))
	} else if sz, ok := m["object_size"].(int); ok {
		n.plan.ObjectSize = plan.Size(int64(sz))
	}
	if params, ok := m["params"].(map[string]interface{}); ok {
		n.plan.Params = params
	}
	return n, nil
}

// Workload is the mix implementation.
type Workload struct {
	name     string
	children []workload.Workload
	weights  []int
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

// Prepopulate sequences child prepopulate calls so each dataset exists before
// Run begins.
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	for _, c := range w.children {
		if err := c.Prepopulate(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

// Run launches each child concurrently with a per-child env whose Threads are
// allocated proportionally to the child's weight. Children share the parent's
// Recorder so per-workload metrics land in the same snapshot.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	total := 0
	for _, wt := range w.weights {
		total += wt
	}
	if total <= 0 {
		return fmt.Errorf("mix %q: total weight must be >0", w.name)
	}
	var wg sync.WaitGroup
	wg.Add(len(w.children))
	errCh := make(chan error, len(w.children))
	for i, c := range w.children {
		threads := env.Threads * w.weights[i] / total
		if threads < 1 {
			threads = 1
		}
		childEnv := *env
		childEnv.Threads = threads
		go func(child workload.Workload, cenv workload.Env) {
			defer wg.Done()
			if err := child.Run(ctx, &cenv); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}(c, childEnv)
	}
	wg.Wait()
	close(errCh)
	var errs []string
	for e := range errCh {
		errs = append(errs, e.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("mix %q child errors: %s", w.name, strings.Join(errs, "; "))
	}
	return nil
}

// Cleanup calls each child in reverse registration order.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	for i := len(w.children) - 1; i >= 0; i-- {
		if err := w.children[i].Cleanup(ctx, env); err != nil {
			return err
		}
	}
	return nil
}
