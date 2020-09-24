package smallben

import (
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

// PauseJobs pause the jobs whose id are in `jobsID`. It returns an error
// of type `gorm.ErrRecordNotFound` if some of the jobs have not been found.
func (s *SmallBen) PauseJobs(jobsID []int64) error {
	// grab the jobs
	// we need to know the cron id
	jobs, err := s.repository.GetRawJobsByIds(jobsID)
	if err != nil {
		return err
	}

	// now update them in the database
	if err = s.repository.PauseJobs(jobs); err != nil {
		return err
	}
	// if here, we have correctly paused them, so we can go on
	// and safely delete them from the database.
	s.scheduler.DeleteJobs(jobs)
	return nil
}

// ResumeTests restarts the Job whose ids are `jobsID`.
// Eventual jobs that were not paused, will keep run smoothly.
// In case of errors during the last steps of the execution,
// the jobs are removed from the scheduler.
func (s *SmallBen) ResumeJobs(jobsID []int64) error {
	// grab the jobs
	jobs, err := s.repository.GetJobsByIdS(jobsID)
	if err != nil {
		return err
	}

	// now, we have to making sure those jobs are not already in the scheduler
	// it's easier, just pick up those whose cron_id = 0
	// because when a job is being paused, it gets a cron_id of 0.
	var finalJobs []JobWithSchedule
	for _, job := range jobs {
		if job.job.CronID == DefaultCronID {
			finalJobs = append(finalJobs, job)
		}
	}

	// ok, now we mark those jobs as resumed
	if err = s.repository.ResumeJobs(finalJobs); err != nil {
		return err
	}

	// and now add them in the scheduler
	s.scheduler.AddJobs(finalJobs)

	// now, update the database by setting the cron id
	if err = s.repository.SetCronId(finalJobs); err != nil {
		// in case there have been errors, we clean up the scheduler too
		// leaving the state unchanged.
		s.scheduler.DeleteJobsWithSchedule(finalJobs)
		return err
	}
	return nil
}

// UpdateSchedule updates the scheduler internal state by changing the `scheduleInfo`
// of the required tests.
// In case of errors, it is guaranteed that, in the worst case, tests will be removed
// from the scheduler will still being in the database with the old schedule.
func (s *SmallBen) UpdateSchedule(scheduleInfo []UpdateSchedule) error {
	// first, we grab all the jobsWithScheduleOld
	jobsWithScheduleOld, err := s.repository.GetJobsByIdS(GetIdsFromUpdateScheduleList(scheduleInfo))
	if err != nil {
		return err
	}

	// the jobsWithScheduleOld with the new required schedule
	jobsWithScheduleNew := make([]JobWithSchedule, len(scheduleInfo))

	// compute the new schedule
	// for the required jobs
	for i, job := range jobsWithScheduleOld {
		newJobRaw := job.job
		newJobRaw.EverySecond = scheduleInfo[i].EverySecond
		newJob, err := newJobRaw.ToJobWithSchedule()
		if err != nil {
			return err
		}
		// ok, now store the new job into the list
		jobsWithScheduleNew[i] = newJob

	}

	// now, remove the jobs from the scheduler
	s.scheduler.DeleteJobsWithSchedule(jobsWithScheduleNew)

	// now, we re-add them to the scheduler
	s.scheduler.AddJobs(jobsWithScheduleNew)

	// and update the database
	if err = s.repository.SetCronIdAndChangeSchedule(jobsWithScheduleNew); err != nil {
		// in case of errors, remove from the scheduler
		s.scheduler.DeleteJobsWithSchedule(jobsWithScheduleNew)
		return err
	}
	return nil
}
