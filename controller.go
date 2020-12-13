package smallben

import (
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

// Config is the struct configuring the overall
// SmallBen object.
type Config struct {
	// SchedulerConfig configures the scheduler
	SchedulerConfig SchedulerConfig
	// Logger is the logger to use.
	Logger logr.Logger
}

// SmallBen is the struct managing the persistent
// scheduler state.
// SmallBen is *goroutine-safe*, since all access are protected by
// a r-w lock.
type SmallBen struct {
	// repository is the storage backend.
	repository Repository
	// scheduler is the cron instance.
	scheduler scheduler
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
	// metrics keeps the prometheus metrics
	// SmallBen export
	metrics metrics
	// logger is the logger used
	logger logr.Logger
}

// New creates a new instance of SmallBen.
// It takes in input the repository and the configuration
// for the scheduler.
func New(repository Repository, config *Config) *SmallBen {
	scheduler := newScheduler(&config.SchedulerConfig)
	return &SmallBen{
		repository: repository,
		scheduler:  scheduler,
		metrics:    newMetrics(),
		logger:     config.Logger,
	}
}

// RegisterMetrics registers the prometheus metrics to registry.
// If registry is nil, then they are registered to the default
// registry.
func (s *SmallBen) RegisterMetrics(registry *prometheus.Registry) error {
	return s.metrics.registerTo(registry)
}

// Start starts the SmallBen, by starting the inner scheduler and filling it
// in with the needed RawJob.
// This call is idempotent and goroutine-safe.
func (s *SmallBen) Start() error {
	s.logger.Info("Starting", "Progress", "InProgress")
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		s.logger.Info("Starting", "Progress", "InProgress", "Details", "Starting")
		// start the scheduler if not started yet.
		s.scheduler.cron.Start()
		// and mark it as started.
		s.started = true
		// now, we fill in the scheduler
		s.logger.Info("Starting", "Progress", "InProgress", "Details", "Filling")
		var err error
		if err = s.fill(); err != nil {
			s.logger.Info("Starting", "Progress", "InProgress", "Details", "Filling done")
		}
		return err
	}
	s.logger.Info("Starting", "Progress", "Done")
	return nil
}

// Stop stops the SmallBen. This call will block until
// all *running* jobs have finished their current execution.
func (s *SmallBen) Stop() {
	s.logger.Info("Stopping", "Progress", "InProgress")
	s.lock.Lock()
	defer s.lock.Unlock()
	ctx := s.scheduler.cron.Stop()
	// Wait on ctx.Done() till all jobsToAdd have finished, then left.
	<-ctx.Done()
	s.logger.Info("Stopping", "Progress", "Done")
}

// AddJobs add `jobs` to the scheduler.
func (s *SmallBen) AddJobs(jobs []Job) error {
	s.logger.Info("Adding jobs", "Progress", "InProgress", "IDs", getIdsFromJobList(jobs))

	s.lock.Lock()
	defer s.lock.Unlock()

	// build the JobWithSchedule struct for each requested Job
	jobsWithSchedule := make([]JobWithSchedule, len(jobs))
	for i, rawJob := range jobs {
		job, err := rawJob.toJobWithSchedule()
		// returning on the first error
		if err != nil {
			s.logger.Error(err, "Adding jobs", "Progress", "Error", "Details", "BuildingJobWithSchedule", "ID", rawJob.ID)
			return err
		}
		jobsWithSchedule[i] = job
	}

	// now, add them to the scheduler
	s.logger.Info("Adding jobs", "Progress", "InProgress", "Details", "AddingToScheduler", "IDs", getIdsFromJobList(jobs))
	s.scheduler.AddJobs(jobsWithSchedule)

	// now, store them in the database
	s.logger.Info("Adding jobs", "Progress", "InProgress", "Details", "AddingToRepository", "IDs", getIdsFromJobList(jobs))

	if err := s.repository.AddJobs(jobsWithSchedule); err != nil {
		// in case of errors, we remove all those jobs from the scheduler
		s.logger.Error(err, "Adding jobs", "Progress", "Error", "Details", "AddingToRepository", "IDs", getIdsFromJobList(jobs))

		s.logger.Info("Adding jobs", "Progress", "Cleaning", "Details", "CleaningScheduler", "IDs", getIdsFromJobList(jobs))
		s.scheduler.DeleteJobsWithSchedule(jobsWithSchedule)
		return err
	}
	// increment the metrics
	s.metrics.addJobs(len(jobs))
	s.logger.Info("Adding jobs", "Progress", "Done", "IDs", getIdsFromJobList(jobs))
	return nil
}

// DeleteJobs deletes permanently jobs according to options.
// It returns an error of type repository.ErrorTypeIfMismatchCount() if the number
// of deleted jobs does not match the expected one.
// It returns an error of type ErrPauseResumeOptionsBad if the
// passed in options are not in a correct format.
func (s *SmallBen) DeleteJobs(options *DeleteOptions) error {
	// check if the struct is valid
	if !options.Valid() {
		return ErrPauseResumeOptionsBad
	}

	s.logger.Info("Deleting jobs", "Progress", "InProgress")

	s.lock.Lock()
	defer s.lock.Unlock()

	// we need for the metrics later
	// since we don't if these jobs
	// are in running or not.
	beforeJobs := s.scheduler.cron.Entries()

	// grab the jobs
	// we need to know the cron id
	jobs, err := s.repository.ListJobs(options)
	if err != nil {
		s.logger.Error(err, "Deleting jobs", "Progress", "Error", "Details", "RetrievingFromRepository", "IDS", getIdsFromJobRawList(jobs))
		return err
	}

	// if no jobs are returned then
	// throws an error and exit
	if len(jobs) == 0 {
		s.logger.Info("Deleting jobs", "Progress", "Error", "Details", "RetrievingFromRepository", "IDs", "No jobs found")
		return s.repository.ErrorTypeIfMismatchCount()
	}

	s.logger.Info("Deleting jobs", "Progress", "InProgress", "IDs", getIdsFromJobRawList(jobs))

	// now delete them
	s.logger.Info("Deleting jobs", "Progress", "InProgress", "Details", "DeletingFromRepository", "IDS", getIdsFromJobRawList(jobs))
	if err = s.repository.DeleteJobsByIds(getIdsFromJobRawList(jobs)); err != nil {
		s.logger.Error(err, "Deleting jobs", "Progress", "Error", "Details", "DeletingFromRepository", "IDS", getIdsFromJobRawList(jobs))
		return err
	}
	s.logger.Info("Deleting jobs ", "Progress", "InProgress", "Details", "DeletingFromScheduler", "IDS", getIdsFromJobRawList(jobs))

	// if here, the deletion from the database was fine
	// so we can safely remove them from the scheduler.
	s.scheduler.DeleteJobs(jobs)

	// update the metrics
	s.metrics.postDelete(beforeJobs, jobs)
	s.logger.Info("Deleting jobs", "Progress", "Done", "IDS", getIdsFromJobRawList(jobs))
	return nil
}

// PauseJobs pauses the jobs according to the filter defined in options.
// It returns an error of type ErrPauseResumeOptionsBad if the options
// are malformed.
// If no jobs matching options are found, an error of type ErrorTypeIfMismatchCount
// is returned.
func (s *SmallBen) PauseJobs(options *PauseResumeOptions) error {
	// check if the struct is correct
	if !options.Valid() {
		return ErrPauseResumeOptionsBad
	}

	s.logger.Info("Pausing jobs", "Progress", "InProgress")

	s.lock.Lock()
	defer s.lock.Unlock()

	// grab the corresponding jobs
	jobs, err := s.repository.ListJobs(options)
	if err != nil {
		s.logger.Error(err, "Pausing jobs", "Progress", "Error", "Details", "RetrievingFromRepository", "IDs", getIdsFromJobRawList(jobs))
		return err
	}

	// if no jobs are returned then
	// throws an error and exit
	if len(jobs) == 0 {
		s.logger.Info("Pausing jobs", "Progress", "Error", "Details", "RetrievingFromRepository", "IDs", "No jobs found")
		return s.repository.ErrorTypeIfMismatchCount()
	}

	s.logger.Info("Pausing jobs", "Progress", "InProgress", "Details", "PausingInRepository", "IDs", getIdsFromJobRawList(jobs))
	// now, we have the list of jobs to act on.
	// now update them in the database
	if err = s.repository.PauseJobs(jobs); err != nil {
		s.logger.Error(err, "Pausing jobs", "Progress", "Error", "Details", "PausingInRepository", "IDs", getIdsFromJobRawList(jobs))
		return err
	}
	// if here, we have correctly paused them, so we can go on
	// and safely delete them from the database.
	s.scheduler.DeleteJobs(jobs)

	// update the metrics
	s.logger.Info("Pausing jobs", "Progress", "InProgress", "Details", "PausingInScheduler", "IDs", getIdsFromJobRawList(jobs))
	s.metrics.pauseJobs(len(jobs))

	s.logger.Info("Pausing jobs", "Progress", "Done", "IDs", getIdsFromJobRawList(jobs))
	return nil
}

// ResumeTests restarts the RawJob according to options.
// Eventual jobsToAdd that were not paused, will keep run smoothly.
// In case of errors during the last steps of the execution,
// the jobsToAdd are removed from the scheduler.
// If no jobs matching options are found, an error of type ErrorTypeIfMismatchCount
// is returned.
func (s *SmallBen) ResumeJobs(options *PauseResumeOptions) error {
	// check if the struct is correct
	if !options.Valid() {
		return ErrPauseResumeOptionsBad
	}

	s.logger.Info("Resume jobs", "Progress", "InProgress")

	s.lock.Lock()
	defer s.lock.Unlock()

	// grab the jobs
	jobs, err := s.repository.ListJobs(options)
	if err != nil {
		s.logger.Error(err, "Resuming jobs", "Progress", "Error", "Details", "RetrievingFromRepository", "IDS", getIdsFromJobRawList(jobs))
		return err
	}

	// if no jobs are returned then
	// throws an error and exit
	if len(jobs) == 0 {
		s.logger.Info("Resuming jobs", "Progress", "Error", "Details", "RetrievingFromRepository", "IDs", "No jobs found")
		return s.repository.ErrorTypeIfMismatchCount()
	}

	// now, we have to making sure those jobsToAdd are not already in the scheduler
	// it's easier, just pick up those whose cron_id = 0
	// because when a rawJob is being paused, it gets a cron_id of 0.
	var finalJobs []JobWithSchedule
	for _, job := range jobs {
		if job.CronID == DefaultCronID {
			jobWithSchedule, err := job.ToJobWithSchedule()
			if err != nil {
				s.logger.Error(err, "Resuming jobs", "Progress", "Error", "Details", "BuildingJobWithSchedule", "ID", job.ID)
				return err
			}
			finalJobs = append(finalJobs, jobWithSchedule)
		}
	}

	// ok, now we mark those jobs as resumed
	s.logger.Info("Resuming jobs", "Progress", "InProgress", "Details", "ResumingInRepository", "IDs", getIdsFromJobRawList(jobs))
	if err = s.repository.ResumeJobs(finalJobs); err != nil {
		return err
	}

	// and now add them in the scheduler
	s.logger.Info("Resuming jobs", "Progress", "InProgress", "Details", "ResumingInScheduler", "IDs", getIdsFromJobRawList(jobs))
	s.scheduler.AddJobs(finalJobs)

	// now, update the database by setting the cron id
	s.logger.Info("Resuming jobs", "Progress", "InProgress", "Details", "SetCronID", "IDs", getIdsFromJobRawList(jobs))
	if err = s.repository.SetCronId(finalJobs); err != nil {
		s.logger.Error(err, "Resuming jobs", "Progress", "Error", "Details", "SetCronID", "IDs", getIdsFromJobRawList(jobs))
		// in case there have been errors, we clean up the scheduler too
		// leaving the state unchanged.
		s.logger.Info("Resuming jobs", "Progress", "Cleaning", "Details", "DeletingFromScheduler", getIdsFromJobRawList(jobs))
		s.scheduler.DeleteJobsWithSchedule(finalJobs)
		return err
	}
	// update the metrics
	s.metrics.resumeJobs(len(jobs))
	s.logger.Info("Resuming jobs", "Progress", "Done", "IDs", getIdsFromJobRawList(jobs))
	return nil
}

// UpdateSchedule updates the scheduler internal state by changing the `scheduleInfo`
// of the required tests.
// In case of errors, it is guaranteed that, in the worst case, tests will be removed
// from the scheduler will still being in the database with the old schedule.
func (s *SmallBen) UpdateSchedule(scheduleInfo []UpdateSchedule) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Info("Updating schedule", "Progress", "InProgress", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
	// first, we grab all the jobs
	jobsWithScheduleOld, err := s.repository.GetJobsByIds(getIdsFromUpdateScheduleList(scheduleInfo))
	if err != nil {
		s.logger.Error(err, "Updating schedule", "Progress", "Error", "Details", "RetrievingFromRepository", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
		return err
	}

	jobsWithScheduleNew := make([]JobWithSchedule, len(scheduleInfo))

	// compute the new schedule
	// for the required jobs
	for i, job := range jobsWithScheduleOld {
		// job is a copy of the original job
		// so it is safe to modify it.
		newJobRaw := job.rawJob
		newJobRaw.CronExpression = scheduleInfo[i].CronExpression
		// build the cron.Schedule object from
		newSchedule, err := scheduleInfo[i].schedule()
		if err != nil {
			s.logger.Error(err, "Updating schedule", "Progress", "Error", "Details", "BuildingJobWithSchedule", "ID", scheduleInfo[i].JobID)
			return err
		}
		// and now, the JobWithSchedule
		// with the new inner rawJob.
		newJob := JobWithSchedule{
			rawJob:   newJobRaw,
			schedule: newSchedule,
			run:      job.run,
			runInput: job.runInput,
		}
		// now store the new rawJob into the list
		jobsWithScheduleNew[i] = newJob
	}

	// now, remove the jobsToAdd from the scheduler
	s.logger.Info("Updating schedule", "Progress", "InProgress", "Details", "DeletingFromScheduler", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
	s.scheduler.DeleteJobsWithSchedule(jobsWithScheduleNew)

	// now, we re-add them to the scheduler
	s.logger.Info("Updating schedule", "Progress", "InProgress", "Details", "AddingToScheduler", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
	s.scheduler.AddJobs(jobsWithScheduleNew)

	// and update the database
	s.logger.Info("Updating schedule", "Progress", "InProgress", "Details", "UpdatingInRepository", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
	if err = s.repository.SetCronIdAndChangeSchedule(jobsWithScheduleNew); err != nil {
		s.logger.Error(err, "Updating schedule", "Progress", "Error", "Details", "UpdatingInRepository", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))

		// in case of errors, remove from the scheduler
		s.logger.Info("Updating schedule", "Progress", "Cleaning", "Details", "DeleteFromScheduler", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
		s.scheduler.DeleteJobsWithSchedule(jobsWithScheduleNew)

		// and update the metrics.
		// Just need to decrement the number of running
		// and increment the number of paused.
		// It is the same as pause()
		s.metrics.pauseJobs(len(jobsWithScheduleNew))
		return err
	}
	// if everything is fine, no need to
	// update the metrics
	s.logger.Info("Updating schedule", "Progress", "Done", "IDs", getIdsFromUpdateScheduleList(scheduleInfo))
	return nil
}

// ListJobs returns the jobs according to `options`.
// It may fail in case of:
// - backend error
// - deserialization error
func (s *SmallBen) ListJobs(options *ListJobsOptions) ([]Job, error) {
	// grab the list of raw jobs
	rawJobs, err := s.repository.ListJobs(options)
	if err != nil {
		return nil, err
	}
	// the array holding the "parsed" jobs
	jobs := make([]Job, len(rawJobs))
	for i, rawJob := range rawJobs {
		// build the parsed job
		job, err := rawJob.toJob()
		// errors in case of deserialization
		if err != nil {
			return nil, err
		}
		// otherwise just add it
		jobs[i] = job
	}
	return jobs, nil
}

// ErrorTypeIfMismatchCount returns the error returned
// during operations where the number of involved jobs
// is different than the number of expected jobs that
// should have been involved.
func (s *SmallBen) ErrorTypeIfMismatchCount() error {
	return s.repository.ErrorTypeIfMismatchCount()
}
