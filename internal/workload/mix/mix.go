// Package mix runs several workloads concurrently with configurable weights,
// implementing PRD §§5.7–5.8 (write-intensive and read-intensive mixes). Child
// thread pools are apportioned by weight without exceeding env.Threads.
package mix

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/workload"
	"golang.org/x/sync/errgroup"
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
	n.weight = workload.IntParam(m, "weight", 1)
	if n.weight <= 0 {
		n.weight = 1
	}
	n.plan.ObjectSize = plan.Size(workload.SizeParam(m, "object_size", 0))
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
// apportioned proportionally to the child's weight without oversubscribing the
// parent thread budget. Children share the parent's Recorder so per-workload
// metrics land in the same snapshot.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if env.Threads <= 0 {
		return fmt.Errorf("mix %q: threads must be >0", w.name)
	}
	total := 0
	for _, wt := range w.weights {
		total += wt
	}
	if total <= 0 {
		return fmt.Errorf("mix %q: total weight must be >0", w.name)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	alloc := allocateThreads(env.Threads, w.weights)
	for i, c := range w.children {
		threads := alloc[i]
		if threads == 0 {
			continue
		}
		childEnv := *env
		childEnv.Threads = threads
		child := c
		group.Go(func() error {
			if err := child.Run(groupCtx, &childEnv); err != nil && groupCtx.Err() == nil {
				return fmt.Errorf("child %q: %w", child.Name(), err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("mix %q child errors: %w", w.name, err)
	}
	return nil
}

func allocateThreads(totalThreads int, weights []int) []int {
	if totalThreads <= 0 || len(weights) == 0 {
		return make([]int, len(weights))
	}
	totalWeight := 0
	for _, w := range weights {
		totalWeight += w
	}
	if totalWeight <= 0 {
		return make([]int, len(weights))
	}
	type remainder struct {
		idx int
		rem int
	}
	alloc := make([]int, len(weights))
	remainders := make([]remainder, 0, len(weights))
	assigned := 0
	for i, w := range weights {
		product := totalThreads * w
		alloc[i] = product / totalWeight
		assigned += alloc[i]
		remainders = append(remainders, remainder{idx: i, rem: product % totalWeight})
	}
	sort.SliceStable(remainders, func(i, j int) bool {
		return remainders[i].rem > remainders[j].rem
	})
	for i := 0; i < totalThreads-assigned && i < len(remainders); i++ {
		alloc[remainders[i].idx]++
	}
	return alloc
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
