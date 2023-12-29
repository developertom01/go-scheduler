package scheduler

import (
	"context"
	"sync"
)

type (
	job       func(ctx context.Context)
	Scheduler interface {
		//Add job to scheduler
		Schedule(j job)

		//Get all pending jobs
		GetPending() int

		//Get all Scheduled jobs - Jobs
		GetScheduled() int

		//Get all finished jobs
		GetFinished() int

		//Wait till n number of jobs is finished
		WaitFinish(n int)

		//Stop scheduler
		Stop()
	}

	fifoScheduler struct {
		mu sync.Mutex

		pending   []job
		finished  int
		scheduled int

		cancel context.CancelFunc
		ctx    context.Context

		resumeCh chan struct{}
		doneCh   chan struct{}

		finishedCond *sync.Cond
	}
)

func NewScheduler() Scheduler {
	f := fifoScheduler{
		resumeCh: make(chan struct{}, 1),
		doneCh:   make(chan struct{}, 1),
	}
	f.finishedCond = sync.NewCond(&f.mu)
	f.ctx, f.cancel = context.WithCancel(context.Background())

	go f.run()
	return &f
}

func (f *fifoScheduler) Schedule(j job) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancel == nil {
		panic("Scheduler is done")
	}
	if len(f.pending) == 0 {
		select {
		case f.resumeCh <- struct{}{}:
		default:
		}
	}
	f.pending = append(f.pending, j)
}

func (f *fifoScheduler) GetScheduled() int {
	return f.scheduled
}

func (f *fifoScheduler) GetFinished() int {
	return f.finished
}

func (f *fifoScheduler) GetPending() int {
	return len(f.pending)
}

func (f *fifoScheduler) WaitFinish(n int) {
	f.finishedCond.L.Lock()
	defer f.finishedCond.L.Unlock()

	for f.finished < n || len(f.pending) != 0 {
		f.finishedCond.Wait()
	}
}

func (f *fifoScheduler) Stop() {
	f.mu.Lock()
	f.cancel()
	f.cancel = nil
	f.mu.Unlock()
	<-f.doneCh
}

func (f *fifoScheduler) run() {
	defer func() {
		close(f.doneCh)
		close(f.resumeCh)
	}()

	for {
		var todo job
		f.mu.Lock()
		if len(f.pending) != 0 {
			f.scheduled++
			todo = f.pending[0]
		}
		f.mu.Unlock()

		if todo == nil {
			select {
			case <-f.resumeCh:
			case <-f.ctx.Done():
				f.mu.Lock()
				pending := f.pending
				f.pending = nil
				f.mu.Unlock()

				//Clean up all pending job
				for _, todo := range pending {
					todo(f.ctx)
				}

			}
		} else {
			todo(f.ctx)
			f.finishedCond.L.Lock()
			f.finished++
			f.finishedCond.Broadcast()
			f.finishedCond.L.Unlock()

		}
	}
}
