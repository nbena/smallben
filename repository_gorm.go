package smallben

import (
	"gorm.io/gorm"
)

const DefaultCronID = int64(0)

// RepositoryGorm implements the Repository
// interface by the means of GORM.
type RepositoryGorm struct {
	db *gorm.DB
}

// NewRepositoryGorm returns an instance of the repository connecting to the given database.
func NewRepositoryGorm(dialector gorm.Dialector, gormConfig *gorm.Config) (RepositoryGorm, error) {
	db, err := gorm.Open(dialector, gormConfig)
	if err != nil {
		return RepositoryGorm{}, err
	}
	return RepositoryGorm{db: db}, nil
}

// AddJobs adds `jobs` to the database. This operation can fail
// if the job serialized fails, or for database errors.
func (r *RepositoryGorm) AddJobs(jobs []JobWithSchedule) error {
	rawJobs := make([]RawJob, len(jobs))
	for i, job := range jobs {
		rawJob, err := job.BuildJob()
		if err != nil {
			return err
		}
		rawJobs[i] = rawJob
	}
	return r.db.Create(&rawJobs).Error
}

// GetJob returns the JobWithSchedule whose id is `jobID`.
// In case the job is not found, an error of type gorm.ErrRecordNotFound is returned.
func (r *RepositoryGorm) GetJob(jobID int64) (JobWithSchedule, error) {
	var rawJob RawJob
	if err := r.db.First(&rawJob, "id = ?", jobID).Error; err != nil {
		return JobWithSchedule{}, err
	}
	// now, convert it to a JobWithSchedule
	job, err := rawJob.ToJobWithSchedule()
	return job, err
}

// PauseJobs pause jobsToAdd whose id are in `jobsToAdd`.
// It returns an error `gorm.ErrRecordNotFound` in case
// the number of updated rows is different then the length of jobsToAdd.
func (r *RepositoryGorm) PauseJobs(jobs []RawJob) error {
	return r.updatePausedField(jobs, true)
}

// PauseJobs resume jobsToAdd whose id are in `jobsToAdd`.
// It returns an error `gorm.ErrRecordNotFound` in case
// the number of updated rows is different then the length of jobsToAdd.
func (r *RepositoryGorm) ResumeJobs(jobs []JobWithSchedule) error {
	result := r.db.Table("jobs").Where("id in ?", GetIdsFromJobsWithScheduleList(jobs)).Updates(map[string]interface{}{"paused": false})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobs)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// GetAllJobsToExecute returns all the jobs whose `paused` field is set to `false`.
func (r *RepositoryGorm) GetAllJobsToExecute() ([]JobWithSchedule, error) {
	paused := false
	// build the struct to make the list query
	// and make the query
	rawJobs, err := r.ListJobs(&ListJobsOptions{
		Paused: &paused,
	})
	if err != nil {
		return nil, err
	}
	// now, convert the raw jobs to instances of JobWithSchedule.
	jobs := make([]JobWithSchedule, len(rawJobs))
	for i, rawJob := range rawJobs {
		job, err := rawJob.ToJobWithSchedule()
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}
	return jobs, nil
}

// GetJobsByIds returns all the jobsToAdd whose ids are in `jobsID`.
// Returns an error of type `gorm.ErrRecordNotFound` in case
// there are less jobsToAdd than the requested ones.
func (r *RepositoryGorm) GetJobsByIds(jobsID []int64) ([]JobWithSchedule, error) {
	// built the struct to do the list query
	// and execute it
	rawJobs, err := r.ListJobs(&ListJobsOptions{
		JobIDs: jobsID,
	})
	if err != nil {
		return nil, err
	}
	jobs := make([]JobWithSchedule, len(rawJobs))
	for i, rawJob := range rawJobs {
		job, err := rawJob.ToJobWithSchedule()
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}
	return jobs, nil
}

// DeleteJobsByIds delete jobs whose id is 'jobsID`, returning an error
// of type gorm.ErrRecordNotFound if the number of deleted jobs is less
// then the length of `jobsID`.
func (r *RepositoryGorm) DeleteJobsByIds(jobsID []int64) error {
	result := r.db.Delete(&RawJob{}, jobsID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobsID)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// SetCronId updates the cron_id field of `jobs`.
func (r *RepositoryGorm) SetCronId(jobs []JobWithSchedule) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		for _, job := range jobs {
			result := tx.Model(&job.rawJob).Update("cron_id", job.rawJob.CronID)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != int64(1) {
				return gorm.ErrRecordNotFound
			}
		}
		return nil
	})
	return err
}

// SetCronIdAndChangeSchedule updates the fields `cron_id` and `cron_expression` of `jobs`.
func (r *RepositoryGorm) SetCronIdAndChangeSchedule(jobs []JobWithSchedule) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		for _, job := range jobs {
			result := tx.Model(&job.rawJob).Updates(map[string]interface{}{"cron_id": job.rawJob.CronID, "cron_expression": job.rawJob.CronExpression})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != int64(1) {
				return gorm.ErrRecordNotFound
			}
		}
		return nil
	})
	return err
}

// ListJobsOptions defines the options
// to use when listing the jobs.
// All options are *combined*, i.e., with an `AND`.
type ListJobsOptions struct {
	// Paused controls the `paused` field.
	// If paused = true list all jobs that have been paused.
	// If paused = false list all jobs that have not been paused.
	// If paused = nil list all jobs no matter if they have been paused or not.
	Paused *bool
	// GroupIDs filters the jobs in the given Group ID.
	// if nil, it is ignored.
	// It makes ListJobs returning all the jobs whose GroupID
	// is in GroupIDs
	GroupIDs []int64
	// SuperGroupIDs filters the jobs by the given Super Group ID.
	// if nil, it is ignored.
	// It makes ListJobs returning all the jobs whose SuperGroupID
	// is in SuperGroupIDs
	SuperGroupIDs []int64
	// JobIDs filters the jobs by the given job ID.
	// This option logically overrides other options
	// since it is the most specific.
	JobIDs []int64
}

// Need to implement the toListOptions interface.
func (o *ListJobsOptions) toListOptions() ListJobsOptions {
	return *o
}

// ListJobs list all jobs using options. If nil, no options will
// be used, thus returning all the jobs.
func (r *RepositoryGorm) ListJobs(options toListOptions) ([]RawJob, error) {
	var jobs []RawJob
	var query = r.db.Session(&gorm.Session{WithConditions: true})
	if options != nil {
		convertedOptions := options.toListOptions()
		if convertedOptions.Paused != nil {
			if *convertedOptions.Paused == true {
				query = query.Where("paused = ?", true)
			} else if *convertedOptions.Paused == false {
				query = query.Where("paused = ?", false)
			}
		}
		if convertedOptions.JobIDs != nil {
			query = query.Where("id in (?)", convertedOptions.JobIDs)
		}
		if convertedOptions.SuperGroupIDs != nil {
			query = query.Where("super_group_id in (?)", convertedOptions.SuperGroupIDs)
		}
		if convertedOptions.GroupIDs != nil {
			query = query.Where("group_id in (?)", convertedOptions.GroupIDs)
		}
	}
	err := query.Find(&jobs).Error
	// a check for gorm.ErrRecordNotFound if we require only the job id
	if options != nil {
		convertedOptions := options.toListOptions()
		if convertedOptions.JobIDs != nil && convertedOptions.SuperGroupIDs == nil &&
			convertedOptions.GroupIDs == nil && convertedOptions.Paused == nil {
			if len(jobs) != len(convertedOptions.JobIDs) {
				err = gorm.ErrRecordNotFound
			}
		}
	}
	return jobs, err
}

func (r *RepositoryGorm) updatePausedField(jobs []RawJob, paused bool) error {
	result := r.db.Table("jobs").Where("id in ?", GetIdsFromJobRawList(jobs)).Updates(map[string]interface{}{"paused": paused, "cron_id": 0})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobs)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}
