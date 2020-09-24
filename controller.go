package smallben

import (
	"context"
	"gorm.io/gorm"
)

// SmallBen is the struct managing the persistent
// scheduler state.
type SmallBen struct {
	repository Repository3
	scheduler  Scheduler
}

// Config regulates the internal working of the scheduler.
type Config struct {
	// DbDialector is the dialector to use to connect to the database
	DbDialector gorm.Dialector
	// DbConfig is the configuration to use to connect to the database.
	DbConfig gorm.Config
}

// NewSmallBen creates a new instance of SmallBen.
// The context is used to connect with the repository.
func NewSmallBen(config *Config) (SmallBen, error) {
	repository, err := NewRepository3(config.DbDialector, &config.DbConfig)
	if err != nil {
		return SmallBen{}, err
	}
	scheduler := NewScheduler()
	return SmallBen{
		repository: repository,
		scheduler:  scheduler,
	}, nil
}

// Start starts the SmallBen, by starting the inner scheduler and filling it
// in with the needed Job.
func (s *SmallBen) Start() error {
	s.scheduler.cron.Start()
	// now, we fill in the scheduler
	return s.Fill()
}

// Stop stops the SmallBen. This call will block until all *running* jobs
// have finished.
func (s *SmallBen) Stop() {
	ctx := s.scheduler.cron.Stop()
	// Wait on ctx.Done() till all jobs have finished, then left.
	<-ctx.Done()
}

// Fill retrieves all the Job to execute from the database
// and then schedules them for execution. In case of errors
// it is guaranteed that *all* the jobs retrieved from the
// database will be cancelled.
func (s *SmallBen) Fill() error {
	// get all the tests
	jobs, err := s.repository.GetAllJobsToExecute()
	// add them to the scheduler to get back the cron_id
	s.scheduler.AddJobs(jobs)
	// now, update the db by updating the cron entries
	err = s.repository.SetCronId(jobs)
	if err != nil {
		// if there is an error, remove them from the scheduler
		s.scheduler.DeleteJobsWithSchedule(jobs)
	}
	return nil
}

// AddJobs add `jobs` to the scheduler.
func (s *SmallBen) AddJobs(jobs []Job) error {
	// build the JobWithSchedule struct
	jobsWithSchedule := make([]JobWithSchedule, len(jobs))
	for i, rawJob := range jobs {
		job, err := rawJob.ToJobWithSchedule()
		if err != nil {
			return err
		}
		jobsWithSchedule[i] = job
	}
	// now, add them to the scheduler
	s.scheduler.AddJobs(jobsWithSchedule)
	// now, store them in the database
	if err := s.repository.AddJobs(jobsWithSchedule); err != nil {
		// in case of errors, we remove all those jobs from the scheduler
		s.scheduler.DeleteJobsWithSchedule(jobsWithSchedule)
		return err
	}
	return nil
}

// DeleteJobs deletes `jobsID` from the scheduler. It returns an error
// of type `gorm.ErrRecordNotFound` if some of the required jobs have not been found.
func (s *SmallBen) DeleteJobs(jobsID []int64) error {
	// grab the jobs
	// we need to know the cron id
	tests, err := s.repository.GetRawJobsByIds(jobsID)
	if err != nil {
		return err
	}

	// now delete them
	if err = s.repository.DeleteJobsByIds(jobsID); err != nil {
		return err
	}

	// if here, the deletion from the database was fine
	// so we can safely remove them from the scheduler.
	s.scheduler.DeleteJobs(tests)
	return nil
}

// PauseTests pause the tests whose id are in `testsID`. It returns an error
// of type `pgx.ErrNoRows` if some of the tests have not been found.
func (s *SmallBen) PauseJobs(ctx context.Context, jobsID []int32) error {
	// grab the tests
	// we need to know the cron id
	tests, err := s.repository.GetJobsByIds(ctx, jobsID)
	if err != nil {
		return err
	}

	// now update them in the database
	if err = s.repository.PauseJobs(ctx, tests); err != nil {
		return err
	}
	// if here, we have correctly paused them, so we can go on
	// and safely delete them from the database.
	s.scheduler.DeleteJobs(tests)
	return nil
}

// ResumeTests restarts the Job whose ids are `testsID`.
func (s *SmallBen) ResumeTests(ctx context.Context, testsID []int32) error {
	// grab the tests
	// we need to know the cron id
	tests, err := s.repository.GetJobsByIds(ctx, testsID)
	if err != nil {
		return err
	}
	// now, build the schedule from the tests recovered from the database.
	testsWithSchedule := make([]JobWithSchedule, len(tests))
	for i, test := range tests {
		testsWithSchedule[i], err = test.ToJobWithSchedule()
		if err != nil {
			return err
		}
	}
	// resume them in the database
	if err = s.repository.ResumeJobs(ctx, tests); err != nil {
		return err
	}

	// and now add them in the scheduler
	s.scheduler.AddTests2(testsWithSchedule)

	// now, update the database by setting the cron id
	if err = s.repository.SetCronIdOfJobsWithSchedule(ctx, testsWithSchedule); err != nil {
		// in case there have been errors, we clean up the scheduler too
		// leaving the state unchanged.
		s.scheduler.DeleteJobs(tests)
		return err
	}
	return nil
}

// UpdateSchedule updates the scheduler internal state by changing the `scheduleInfo`
// of the required tests.
// In case of errors, it is guaranteed that, in the worst case, tests will be removed
// from the scheduler will still being in the database with the old schedule.
func (s *SmallBen) UpdateSchedule(ctx context.Context, scheduleInfo []UpdateSchedule) error {
	// first, we grab all the tests
	tests, err := s.repository.GetJobsByIds(ctx, GetIdsFromUpdateScheduleList(scheduleInfo))
	if err != nil {
		return err
	}

	// the tests with the new required schedule
	testsWithScheduleNew := make([]JobWithSchedule, len(scheduleInfo))
	// the tests with the old schedule
	testsWithScheduleOld := make([]JobWithSchedule, len(scheduleInfo))

	// now, we compute the new schedule while also
	// keeping a copy of the old one
	for i, test := range tests {
		// compute the schedule of the old one
		testWithScheduleOld, err := test.ToJobWithSchedule()
		if err != nil {
			// should never happen, but...
			return err
		}
		// insert the test with the old schedule in the list
		testsWithScheduleOld[i] = testWithScheduleOld

		// make a copy of it
		testRawNew := testWithScheduleOld.Job
		// and update the everySecond parameter
		testRawNew.EverySecond = scheduleInfo[i].EverySecond
		// in order to compute the new schedule
		testWithScheduleNew, err := testRawNew.ToJobWithSchedule()
		if err != nil {
			return err
		}
		// and insert them in the list
		testsWithScheduleNew[i] = testWithScheduleNew
	}

	// now, we remove the tests from scheduler
	// it is safe to remove tests from the scheduler even if they
	// are not in the scheduler.
	// It is ok to delete tests even if they are not in the scheduler.
	s.scheduler.DeleteJobsWithSchedule(testsWithScheduleNew)

	// now, we (re-)add them to the scheduler
	s.scheduler.AddTests2(testsWithScheduleNew)

	// now, we update the cron id in the database and also we update
	// the schedule. And we're done
	if err = s.repository.SetCronIdAndChangeSchedule(ctx, testsWithScheduleNew); err != nil {
		// in this case, we remove them from the scheduler
		s.scheduler.DeleteJobsWithSchedule(testsWithScheduleNew)
		return err
	}
	return nil
}
