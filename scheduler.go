package smallben

import (
	"fmt"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cron *cron.Cron
}

// Returns a new Scheduler.
func NewScheduler() Scheduler {
	return Scheduler{
		cron: cron.New(cron.WithSeconds()),
	}
}

// AddJobs adds `jobs` to the scheduler. This function never fails and updates
// the input array with the `CronID`.
func (s *Scheduler) AddJobs(jobs []JobWithSchedule) {

	for _, job := range jobs {
		runFunc := job.run
		runFuncInput := job.runInput

		entryID := s.cron.Schedule(job.schedule, cron.FuncJob(func() {
			runFunc.Run(runFuncInput)
		}))

		job.job.CronID = int64(entryID)
	}
}

// DeleteJobsWithSchedule remove `jobs` from the scheduler. This function never fails.
func (s *Scheduler) DeleteJobsWithSchedule(jobs []JobWithSchedule) {
	for _, job := range jobs {
		s.cron.Remove(cron.EntryID(job.job.CronID))
	}
}

// DeleteJobs remove `jobs` from the scheduler.
func (s *Scheduler) DeleteJobs(jobs []Job) {
	for _, job := range jobs {
		s.cron.Remove(cron.EntryID(job.CronID))
	}
}

type runFunctionInput struct {
	jobID        int32
	groupID      int32
	superGroupID int32
}

func (r *runFunctionInput) Run() {
	fmt.Printf("Im running\n")
}
