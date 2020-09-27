package smallben

import (
	"errors"
	"gorm.io/gorm"
)

var ErrPauseResumeOptionsBad = errors.New("wrong combination of the fields of PauseResumeOptions")

// Config regulates the internal working of the scheduler.
type Config struct {
	// DbDialector is the dialector to use to connect to the database
	DbDialector gorm.Dialector
	// DbConfig is the configuration to use to connect to the database.
	DbConfig gorm.Config
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

// PauseResumeOptions governs the behavior
// of the PauseJobs and ResumeJobs methods.
type PauseResumeOptions struct {
	// JobIDs specifies which jobs will be
	// paused or resumed. This option is ignored
	// if it is nil. If it is option is
	// set, but also other options are set, an error
	// of type ErrPauseResumeOptionsBad is returned.
	JobIDs []int64
	// GroupIDs specifies the group ids
	// whose jobs will be paused or resumed.
	GroupIDs []int64
	// SuperGroupIDs specifies the super group ids
	// whose jobs will be paused or resumed.
	SuperGroupIDs []int64
}

// Valid checks if o is valid.
func (o *PauseResumeOptions) Valid() bool {
	if o.JobIDs != nil && (o.GroupIDs != nil || o.SuperGroupIDs != nil) {
		return false
	}
	if o.JobIDs == nil && o.GroupIDs == nil && o.SuperGroupIDs == nil {
		return false
	}
	return true
}

// getJobsFromOptions returns all the jobs according to options.
func (s *SmallBen) getJobsFromOptions(options *PauseResumeOptions) ([]RawJob, error) {
	var jobs []RawJob
	var err error

	if options.JobIDs != nil {
		jobs, err = s.repository.GetRawJobsByIds(options.JobIDs)
	} else if options.GroupIDs != nil && options.SuperGroupIDs != nil {
		jobs, err = s.repository.ListJobs(&ListJobsOptions{
			GroupIDs:      options.GroupIDs,
			SuperGroupIDs: options.SuperGroupIDs,
		})
	} else if options.GroupIDs != nil {
		jobs, err = s.repository.ListJobs(&ListJobsOptions{
			GroupIDs: options.GroupIDs,
		})
	} else {
		jobs, err = s.repository.ListJobs(&ListJobsOptions{
			SuperGroupIDs: options.SuperGroupIDs,
		})
	}
	return jobs, err
}
