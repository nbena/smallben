package smallben

import (
	"gorm.io/gorm"
)

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

// AddJobs adds `jobs` to the database. This operation can fail
// if job serialization fails, or for database errors.
func (r *Repository3) AddJobs(jobs []JobWithSchedule) error {
	rawJobs := make([]Job, len(jobs))
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
	var rawJob Job
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

// PauseJobs pause jobs whose id are in `jobs`.
// It returns an error `gorm.ErrRecordNotFound` in case
// the number of updated rows is different then the length of jobs.
func (r *Repository3) PauseJobs(jobs []Job) error {
	return r.updatePausedField(jobs, true)
}

// PauseJobs resume jobs whose id are in `jobs`.
// It returns an error `gorm.ErrRecordNotFound` in case
// the number of updated rows is different then the length of jobs.
func (r *Repository3) ResumeJobs(jobs []Job) error {
	return r.updatePausedField(jobs, false)
}

// GetAllJobsToExecute returns all the jobs whose `paused` field is set to `false`.
func (r *Repository3) GetAllJobsToExecute() ([]JobWithSchedule, error) {
	var rawJobs []Job
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

func (r *Repository3) GetRawJobsByIds(jobsID []int64) ([]Job, error) {
	var rawJobs []Job
	result := r.db.Find(&rawJobs, "id in ?", jobsID)
	if result.Error != nil {
		return nil, result.Error
	}
	if len(rawJobs) != len(jobsID) {
		return nil, gorm.ErrRecordNotFound
	}
	return rawJobs, nil
}

func (r *Repository3) DeleteJobsByIds(jobsID []int64) error {
	result := r.db.Delete(&Job{}, jobsID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobsID)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// SetCronId updates the cron_id field of `jobs`.
func (r *Repository3) SetCronId(jobs []JobWithSchedule) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		for _, job := range jobs {
			result := tx.Model(&job).Update("cron_id", job.job.CronID)
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
			result := tx.Model(&job).Updates(map[string]interface{}{"cron_id": job.job.CronID, "every_second": job.job.EverySecond})
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

func (r *Repository3) updatePausedField(jobs []Job, paused bool) error {
	result := r.db.Table("jobs").Where("id in ?", GetIdsFromJobsList(jobs)).Updates(map[string]interface{}{"paused": paused})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobs)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}
