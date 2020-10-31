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
// in case the number of updated items is different than
// the length of jobs to add.
func (m *MemoryRepository) ResumeJobs(jobs []JobWithSchedule) error {
	rawJobs := make([]RawJob, len(jobs))
	for i, job := range jobs {
		rawJobs[i] = job.rawJob
	}
	return m.updatePausedField(rawJobs, false)
}

// GetJobsByIds returns all the jobsToAdd whose ids are in `jobsID`.
// Returns an error of type ErrorTypeIfMismatchCount() in case
// there are less jobs than the requested ones.
func (m *MemoryRepository) GetJobsByIds(jobsID []int64) ([]JobWithSchedule, error) {
	rawJobs, err := m.ListJObs(&ListJobsOptions{
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

// DeleteJobsByIds delete jobs whose ids are 'jobsID`, returning an error
// of type ErrorTypeIfMismatchCount() if the number of deleted jobs is
// less than the length of jobsID.
func (m *MemoryRepository) DeleteJobsByIds(jobsID []int64) error {
	count := 0
	for _, jobID := range jobsID {
		// grab the item to see if it exists
		if _, ok := m.data[jobID]; !ok {
			// and delete it incrementing our counter
			delete(m.data, jobID)
			count += 1
		}
	}
	// check if the number of deleted jobs
	// matches the required number of jobs to delete.
	if count != len(jobsID) {
		return ErrRecordNotFound
	}
	return nil
}

// SetCronId updates the cron_id field of `jobs`.
// It updates all the jobs contained in `jobs`, returning
// an error of type ErrorTypeIfMismatchCount() if the number
// of updated jobs is less than the length of jobs.
// Still, it does NOT stop on first error.
func (m *MemoryRepository) SetCronId(jobs []JobWithSchedule) error {
	count := 0
	for _, job := range jobs {
		gotJob, ok := m.data[job.rawJob.ID]
		if ok {
			gotJob.rawJob.CronID = job.rawJob.CronID
			m.data[job.rawJob.ID] = gotJob
			count += 1
		}
	}
	// check that the number of updated jobs is correct.
	if count != len(jobs) {
		return ErrRecordNotFound
	}
	return nil
}

// ListJObs filters the current map according to options.
// It never fails except for the following case:
// - the only required filter option is by job id AND
// - the length of the returned list is different than the length of
// 	 the required job id.
// They should be, in fact, equal, since job ids are unique,
func (m *MemoryRepository) ListJObs(options ToListOptions) ([]RawJob, error) {
	var jobs []RawJob
	var err error
	for _, job := range m.data {
		if options != nil {
			// this flag is invalidated
			// when the first condition does not match
			flag := true
			convertedOptions := options.toListOptions()
			if convertedOptions.Paused != nil {
				if job.rawJob.Paused != *convertedOptions.Paused {
					flag = false
				}
			}
			// check the JobIDs option
			// but only if the flag is not already invalid
			if convertedOptions.JobIDs != nil && flag {
				// this flag is set to true if
				// the current job has a job id which is equal
				// to an element in convertedOptions.JobIDs.
				// It is then used to set the value of `flag`.
				found := false
				for _, jobID := range convertedOptions.JobIDs {
					if job.rawJob.ID == jobID {
						found = true
						break
					}
				}
				// now set the flag
				if !found {
					flag = false
				}
			}
			// check the GroupIDs option
			// but only if the flag is not already invalid
			if convertedOptions.GroupIDs != nil && flag {
				// this flag works as the previous one.
				found := false
				for _, groupID := range convertedOptions.GroupIDs {
					if job.rawJob.GroupID == groupID {
						found = true
						break
					}
				}
				// now set the flag
				if !found {
					flag = false
				}
			}
			// check the SuperGroupIDs option
			if convertedOptions.SuperGroupIDs != nil && flag {
				// this flag works as the previous one.
				found := false
				for _, superGroupID := range convertedOptions.SuperGroupIDs {
					if job.rawJob.SuperGroupID == superGroupID {
						found = true
						break
					}
				}
				// now set the flag
				if !found {
					flag = false
				}
			}
			// at this point, we have evaluated all
			// the possible conditions. Since the first invalid match
			// has set our flag to false, then there is full match only if
			// flag = true
			if flag {
				jobs = append(jobs, job.rawJob)
			}
		} else {
			// if no options, then just add the job
			jobs = append(jobs, job.rawJob)
		}
	}
	// at this point, we have to make sure that the
	// returned list matches the required input, i.e.,
	// if the user has requested only a filter by job id
	// we have to check that the length of the returned
	// list is equal to the length of required list of job id,
	// since job ids are unique.
	if options != nil {
		convertedOptions := options.toListOptions()
		if convertedOptions.JobIDs != nil && convertedOptions.SuperGroupIDs == nil &&
			convertedOptions.GroupIDs == nil && convertedOptions.Paused == nil {
			if len(jobs) != len(convertedOptions.JobIDs) {
				err = ErrRecordNotFound
			}
		}
	}
	return jobs, err
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
