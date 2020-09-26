package smallben

import (
	"gorm.io/gorm"
	"sync"
)

// SmallBen is the struct managing the persistent
// scheduler state.
// SmallBen is *goroutine-safe*, since all access are protected by
// a r-w lock.
type SmallBen struct {
	// repository is where we store jobsToAdd.
	repository Repository3
	// scheduler is the cron instance.
	scheduler Scheduler
	// lock protects access to each operation
	// of SmallBen.
	lock sync.RWMutex
	// filled specifies if SmallBen
	// has been filled. In that case, subsequent calls
	// to the fill method does not change the state.
	filled bool
	// started specifies if SmallBen has been already
	// started. In that case, subsequent calls to the Start
	// method does not start the scheduler once again.
	started bool
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
func NewSmallBen(config *Config) (*SmallBen, error) {
	repository, err := NewRepository3(config.DbDialector, &config.DbConfig)
	if err != nil {
		return nil, err
	}
	scheduler := NewScheduler()
	return &SmallBen{
		repository: repository,
		scheduler:  scheduler,
	}, nil
}

// Start starts the SmallBen, by starting the inner scheduler and filling it
// in with the needed RawJob.
// This call is idempotent and goroutine-safe.
func (s *SmallBen) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		// start the scheduler if not started yet.
		s.scheduler.cron.Start()
		// and mark it as started.
		s.started = true
		// now, we fill in the scheduler
		return s.fill()
	}
	return nil
}

// Stop stops the SmallBen. This call will block until all *running* jobsToAdd
// have finished.
func (s *SmallBen) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	ctx := s.scheduler.cron.Stop()
	// Wait on ctx.Done() till all jobsToAdd have finished, then left.
	<-ctx.Done()
}

// fill retrieves all the RawJob to execute from the database
// and then schedules them for execution. In case of errors
// it is guaranteed that *all* the jobsToAdd retrieved from the
// database will be cancelled.
// This method is *idempotent*, call it every time you want,
// and the scheduler won't be filled in twice.
func (s *SmallBen) fill() error {
	if !s.filled {
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
		s.filled = true
	}
	return nil
}

// AddJobs add `jobsToAdd` to the scheduler.
func (s *SmallBen) AddJobs(jobs []Job) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// build the JobWithSchedule struct for each requested job
	jobsWithSchedule := make([]JobWithSchedule, len(jobs))
	for i, rawJob := range jobs {
		job, err := rawJob.toJobWithSchedule()
		// returning on the first error
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
// of type `gorm.ErrRecordNotFound` if some of the required jobsToAdd have not been found.
func (s *SmallBen) DeleteJobs(jobsID []int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
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

// PauseJobs pause the jobsToAdd whose id are in `jobsID`. It returns an error
// of type `gorm.ErrRecordNotFound` if some of the jobsToAdd have not been found.
func (s *SmallBen) PauseJobs(jobsID []int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
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

// ResumeTests restarts the RawJob whose ids are `jobsID`.
// Eventual jobsToAdd that were not paused, will keep run smoothly.
// In case of errors during the last steps of the execution,
// the jobsToAdd are removed from the scheduler.
func (s *SmallBen) ResumeJobs(jobsID []int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// grab the jobsToAdd
	jobs, err := s.repository.GetJobsByIdS(jobsID)
	if err != nil {
		return err
	}

	// now, we have to making sure those jobsToAdd are not already in the scheduler
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
	s.lock.Lock()
	defer s.lock.Unlock()
	// first, we grab all the jobs
	jobsWithScheduleOld, err := s.repository.GetJobsByIdS(GetIdsFromUpdateScheduleList(scheduleInfo))
	if err != nil {
		return err
	}

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

	// now, remove the jobsToAdd from the scheduler
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
