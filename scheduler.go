package smallben

import (
	"github.com/robfig/cron/v3"
)

// Scheduler is the struct wrapping cron.
type Scheduler struct {
	cron *cron.Cron
}

// Returns a new Scheduler.
// The Scheduler supports scheduling
// on a per-second basis.
func NewScheduler() Scheduler {
	return Scheduler{
		cron: cron.New(cron.WithSeconds()),
	}
}

// AddJobs adds `jobs` to the scheduler.
// This function never fails and updates
// the input array with the `CronID`.
func (s *Scheduler) AddJobs(jobs []JobWithSchedule) {

	for i := range jobs {
		runFunc := jobs[i].run
		runFuncInput := jobs[i].runInput

		entryID := s.cron.Schedule(jobs[i].schedule, cron.FuncJob(func() {
			runFunc.Run(runFuncInput)
		}))

		jobs[i].rawJob.CronID = int64(entryID)
	}
}

// DeleteJobsWithSchedule remove `jobs` from the scheduler.
// This function never fails.
func (s *Scheduler) DeleteJobsWithSchedule(jobs []JobWithSchedule) {
	for _, job := range jobs {
		s.cron.Remove(cron.EntryID(job.rawJob.CronID))
	}
}

// DeleteJobs remove `jobs` from the scheduler.
// This function never fails, if the job is not
// in the scheduler, then this is no-op.
// It, basically, inherits the behavior
// of the inner scheduler.
func (s *Scheduler) DeleteJobs(jobs []RawJob) {
	for _, job := range jobs {
		s.cron.Remove(cron.EntryID(job.CronID))
	}
}
