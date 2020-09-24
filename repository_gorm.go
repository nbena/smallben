package smallben

import (
	"gorm.io/gorm"
)

type Repository3 struct {
	db gorm.DB
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

func (r *Repository3) PauseJobs(jobs []Job) error {
	result := r.db.Table("jobs").Where("id in ?", GetIdsFromJobsList(jobs)).Updates(map[string]interface{}{"paused": true})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(jobs)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}
