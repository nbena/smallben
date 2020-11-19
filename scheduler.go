package smallben

import (
	"github.com/robfig/cron/v3"
	"time"
)

// Scheduler is the struct wrapping cron.
type Scheduler struct {
	cron *cron.Cron
}

// SchedulerConfig contains the configuration
// for the scheduler.
// It provides most of the option for configuring
// cron through this struct instead of using the Option-style
// pattern.
type SchedulerConfig struct {
	// DelayIfStillRunning delays a job starting
	// if that job has not finished yet.
	// Equivalent to attaching: https://pkg.go.dev/github.com/robfig/cron/v3#DelayIfStillRunning
	DelayIfStillRunning bool
	// SkipIfStillRunning skips a job starting
	// if that job has not finished yet.
	// Equivalent to attaching: https://pkg.go.dev/github.com/robfig/cron/v3#SkipIfStillRunning
	SkipIfStillRunning bool
	// WithSeconds enable seconds-grained scheduling.
	// Equivalent to: https://pkg.go.dev/github.com/robfig/cron/v3#WithSeconds
	WithSeconds bool
	// WithParser specifies the scheduler parser to use.
	// If nil, the default one is used.
	// Equivalent to: https://pkg.go.dev/github.com/robfig/cron/v3#WithParser
	WithParser cron.ScheduleParser
	// WithLocation sets the location for the scheduler.
	// Equivalent to: https://pkg.go.dev/github.com/robfig/cron/v3#WithLocation
	WithLocation *time.Location
	// WithLogger specifies the logger to use.
	// Equivalent to: https://pkg.go.dev/github.com/robfig/cron/v3#WithLogger
	WithLogger cron.Logger
}

// toOptions returns a list of cron.Option to apply
// from the given configuration
func (c *SchedulerConfig) toOptions() []cron.Option {
	var options []cron.Option
	if c.WithSeconds {
		options = append(options, cron.WithSeconds())
	}
	if c.WithLogger != nil {
		options = append(options, cron.WithLogger(c.WithLogger))
	}
	if c.WithLocation != nil {
		options = append(options, cron.WithLocation(c.WithLocation))
	}
	return options
}

// Returns a new Scheduler.
// It takes in input the options to configure the
// inner scheduler.
func NewScheduler(config *SchedulerConfig) Scheduler {
	// build the options from the configuration.
	options := config.toOptions()
	// create the scheduler struct...
	return Scheduler{
		// by passing it the options...
		cron: cron.New(options...),
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
