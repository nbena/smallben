package smallben

import (
	"errors"
	"time"
)

type MemoryRepository struct {
	data map[int64]JobWithSchedule
}

var ErrRecordNotFound = errors.New("the requested record has not been found")

// AddJobs adds `job` to the in-memory data structure.
// It never fails. Old jobs are eventually overwritten.
func (m *MemoryRepository) AddJobs(jobs []JobWithSchedule) error {
	for _, job := range jobs {
		// we are operating over a copy of the array
		// so we can modify it.
		job.rawJob.CreatedAt = time.Now()
		job.rawJob.UpdatedAt = time.Now()
		m.data[job.rawJob.ID] = job
	}
	return nil
}

// GetJob returns the JobWithSchedule whose id is `jobID`.
// In case the job is not found, an error of type ErrorTypeIfMismatchCount() is returned.
func (m *MemoryRepository) GetJob(jobID int64) (JobWithSchedule, error) {
	job, ok := m.data[jobID]
	if !ok {
		return JobWithSchedule{}, ErrRecordNotFound
	}
	return job, nil
}

// PauseJobs pauses jobs whose id are in `jobs`.
// It returns an error of type ErrorTypeIfMismatchCount()
// in case the number of updated items is different than the
// length of jobs to add.
// It updates all the jobs it can, i.e., does not finish
// when it encounters the first non-existing job.
func (m *MemoryRepository) PauseJobs(jobs []RawJob) error {
	return m.updatePausedField(jobs, true)
}

// ResumeJobs resume jobs whose id are in `jobs`.
// It returns an error of type ErrorTypeIfMismatchCount()
// In case the number of updated items is different than
// the length of jobs to add.
func (m *MemoryRepository) ResumeJobs(jobs []JobWithSchedule) error {
	rawJobs := make([]RawJob, len(jobs))
	for i, job := range jobs {
		rawJobs[i] = job.rawJob
	}
	return m.updatePausedField(rawJobs, false)
}

func (m *MemoryRepository) updatePausedField(jobs []RawJob, value bool) error {
	var err error = nil
	for _, job := range jobs {
		jobToUpdate, ok := m.data[job.ID]
		// if found, then update
		if ok {
			jobToUpdate.rawJob.Paused = value
			jobToUpdate.rawJob.UpdatedAt = time.Now()
			m.data[job.ID] = jobToUpdate
		} else {
			// else fill in the error variable
			// don't care if we update it multiple times.
			err = ErrRecordNotFound
		}

	}
	return err
}
