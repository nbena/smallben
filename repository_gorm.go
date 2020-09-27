package smallben

import (
	"gorm.io/gorm"
)

const DefaultCronID = int64(0)

type Repository3 struct {
	db *gorm.DB
}

// NewRepository3 returns an instance of the repository connecting to the given database.
func NewRepository3(dialector gorm.Dialector, gormConfig *gorm.Config) (Repository3, error) {
	db, err := gorm.Open(dialector, gormConfig)
	if err != nil {
		return Repository3{}, err
	}
	return Repository3{db: db}, nil
}

// AddJobs adds `jobsToAdd` to the database. This operation can fail
// if rawJob serialization fails, or for database errors.
func (r *Repository3) AddJobs(jobs []JobWithSchedule) error {
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

// GetJob returns the rawJob whose id is `jobID`.
func (r *Repository3) GetJob(jobID int64) (JobWithSchedule, error) {
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
func (r *Repository3) PauseJobs(jobs []RawJob) error {
	return r.updatePausedField(jobs, true)
}

// PauseJobs resume jobsToAdd whose id are in `jobsToAdd`.
// It returns an error `gorm.ErrRecordNotFound` in case
// the number of updated rows is different then the length of jobsToAdd.
func (r *Repository3) ResumeJobs(jobs []JobWithSchedule) error {
	result := r.db.Table("jobs").Where("id in ?", GetIdsFromJobsWithScheduleList(jobs)).Updates(map[string]interface{}{"paused": false})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobs)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// GetAllJobsToExecute returns all the jobsToAdd whose `paused` field is set to `false`.
func (r *Repository3) GetAllJobsToExecute() ([]JobWithSchedule, error) {
	var rawJobs []RawJob
	if err := r.db.Find(&rawJobs, "paused = false").Error; err != nil {
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

// GetRawJobsByIds returns all the jobsToAdd whose ids are in `jobsID`.
// Returns an error of kind `gorm.ErrRecordNotFound` in case
// there are less jobsToAdd than the requested ones.
func (r *Repository3) GetRawJobsByIds(jobsID []int64) ([]RawJob, error) {
	var rawJobs []RawJob
	result := r.db.Find(&rawJobs, "id in ?", jobsID)
	if result.Error != nil {
		return nil, result.Error
	}
	if len(rawJobs) != len(jobsID) {
		return nil, gorm.ErrRecordNotFound
	}
	return rawJobs, nil
}

// GetJobsByIds returns all the jobsToAdd whose ids are in `jobsID`.
// Returns an error of kind `gorm.ErrRecordNotFound` in case
// there are less jobsToAdd than the requested ones.
func (r *Repository3) GetJobsByIdS(jobsID []int64) ([]JobWithSchedule, error) {
	rawJobs, err := r.GetRawJobsByIds(jobsID)
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

func (r *Repository3) DeleteJobsByIds(jobsID []int64) error {
	result := r.db.Delete(&RawJob{}, jobsID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobsID)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// SetCronId updates the cron_id field of `jobsToAdd`.
func (r *Repository3) SetCronId(jobs []JobWithSchedule) error {
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

func (r *Repository3) SetCronIdAndChangeSchedule(jobs []JobWithSchedule) error {
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
// All options are *combined*.
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
	// This option override other options.
	JobIDs []int64
}

// Need to implement the toListOptions interface.
func (o *ListJobsOptions) toListOptions() ListJobsOptions {
	return *o
}

// ListJobs list all jobs using options. If nil, no options will
// be used returning all the jobs.
func (r *Repository3) ListJobs(options toListOptions) ([]RawJob, error) {
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

func (r *Repository3) updatePausedField(jobs []RawJob, paused bool) error {
	result := r.db.Table("jobs").Where("id in ?", GetIdsFromJobRawList(jobs)).Updates(map[string]interface{}{"paused": paused, "cron_id": 0})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobs)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}
