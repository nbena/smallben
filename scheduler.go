package smallben

import (
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

// DeleteJobsWithSchedule remove `jobs` from the scheduler. This function never fails.
func (s *Scheduler) DeleteJobsWithSchedule(jobs []JobWithSchedule) {
	for _, job := range jobs {
		s.cron.Remove(cron.EntryID(job.rawJob.CronID))
	}
}

// DeleteJobs remove `jobsToAdd` from the scheduler.
func (s *Scheduler) DeleteJobs(jobs []RawJob) {
	for _, job := range jobs {
		s.cron.Remove(cron.EntryID(job.CronID))
	}
}
