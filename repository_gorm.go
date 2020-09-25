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
// if job serialization fails, or for database errors.
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

// GetJob returns the job whose id is `jobID`.
func (r *Repository3) GetJob(jobID int64) (JobWithSchedule, error) {
	var rawJob RawJob
	if err := r.db.First(&rawJob, "id = ?", jobID).Error; err != nil {
		return JobWithSchedule{}, err
	}
	// now, convert it to a JobWithSchedule
	job, err := rawJob.ToJobWithSchedule()
	if err != nil {
		return JobWithSchedule{}, err
	}
	return job, nil
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
			result := tx.Model(&job.job).Update("cron_id", job.job.CronID)
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
			result := tx.Model(&job.job).Updates(map[string]interface{}{"cron_id": job.job.CronID, "every_second": job.job.EverySecond})
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
