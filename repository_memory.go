package smallben

import (
	"errors"
	"sync"
)

// ErrorRecordNotFound is used to specify the type of errors
// returned by this repository.
var ErrorRecordNotFound = errors.New("record not found")

type RepositoryMemory struct {
	db sync.Map
}

func (r *RepositoryMemory) ErrorTypeIfMismatchCount() error {
	return ErrorRecordNotFound
}

// AddJobs add jobs to the in-memory database. It only fails
// in case the serialization fails.
func (r *RepositoryMemory) AddJobs(jobs []JobWithSchedule) error {
	rawJobs := make([]RawJob, len(jobs))
	for i, job := range jobs {
		rawJob, err := job.BuildJob()
		if err != nil {
			return err
		}
		rawJobs[i] = rawJob
	}
	for i := range rawJobs {
		r.db.Store(rawJobs[i].ID, rawJobs[i])
	}
	return nil
}

// GetJob returns the job whose id is `jobID`.
func (r *RepositoryMemory) GetJob(jobID int64) (JobWithSchedule, error) {
	rawJobInterface, ok := r.db.Load(jobID)
	if !ok {
		return JobWithSchedule{}, ErrorRecordNotFound
	}
	rawJob := rawJobInterface.(RawJob)
	return rawJob.ToJobWithSchedule()
}

func (r *RepositoryMemory) PauseJobs(jobs []RawJob) error {
	gotJobs, err := r.retrieveMany(GetIdsFromJobRawList(jobs))
	if err != nil {
		return err
	}
	for i := range gotJobs {
		gotJobs[i].Paused = true
	}
	return nil
}

func (r *RepositoryMemory) retrieveMany(ids []int64) ([]RawJob, error) {
	jobsInterface := make([]interface{}, len(ids))
	for i := range ids {
		jobInterface, ok := r.db.Load(ids[i])
		if !ok {
			return nil, ErrorRecordNotFound
		}
		jobsInterface[i] = jobInterface
	}
	gotJobs := r.interfaceToRaw(jobsInterface)
	if len(gotJobs) != len(ids) {
		return nil, ErrorRecordNotFound
	}
	return gotJobs, nil
}

func (r *RepositoryMemory) interfaceToRaw(items []interface{}) []RawJob {
	jobs := make([]RawJob, len(items))
	for i := range items {
		jobs[i] = items[i].(RawJob)
	}
	return jobs
}
