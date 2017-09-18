// Package errgroup provides synchronization, error propagation, and Context
// cancelation for groups of goroutines working on subtasks of a common task.

// TODO: Attirbute both oklog and Go Authors.
package errgroup

import (
	"context"
	"sync"
)

// A Group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero Group is valid and does not cancel on error.
type Group struct {
	cancel  func()
	errOnce sync.Once
	err     error

	funcs   []func() error
	workers int
}

// WithContext returns a new Group and an associated Context derived from ctx.
//
// The derived Context is canceled the first time a function passed to Go
// returns a non-nil error or the first time Wait returns, whichever occurs
// first.
func New(ctx context.Context, workers int) (*Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{
		cancel:  cancel,
		workers: workers,
	}, ctx
}

// Add an function to the group.
func (g *Group) Add(f func() error) {
	g.funcs = append(g.funcs, f)
}

// Run all functions concurrently.
// Run only returns when all functions have exited.
// Run returns the error returned by the first exiting actor.
func (g *Group) Run() error {
	if len(g.funcs) == 0 {
		return nil
	}

	errors := make(chan error)
	fChan := make(chan func() error)

	// Start the worker pool.
	for i := 0; i < g.workers; i++ {
		go func(fc <-chan func() error, ec chan<- error, i int) {
			for f := range fc {
				ec <- f()
			}
		}(fChan, errors, i)
	}

	// Send work to the workers.
	go func() {
		for _, f := range g.funcs {
			fChan <- f
		}
		close(fChan)
	}()

	// Wait for the workers to return.
	for i := 0; i < len(g.funcs); i++ {
		if err := <-errors; err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if g.cancel != nil {
					g.cancel()
				}
			})
		}
	}
	close(errors)

	// Return the original error.
	if g.cancel != nil {
		g.cancel()
	}
	return g.err
}
