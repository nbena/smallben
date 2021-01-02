package smallben

// fill retrieves all the RawJob to execute from the database
// and then schedules them for execution. In case of errors
// it is guaranteed that *all* the jobs retrieved from the
// database will be cancelled.
// This method is *idempotent*, call it every time you want,
// and the scheduler won't be filled in twice.
func (s *SmallBen) fill() error {
	if !s.filled {
		// filling in the metrics
		if err := s.fillMetrics(); err != nil {
			return err
		}
		// get all the tests
		jobs, err := s.repository.GetAllJobsToExecute()
		if err != nil {
			return err
		}
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

// ToListOptions is an interface implemented
// by structs that can be converted to a ListOptions struct.
type ToListOptions interface {
	toListOptions() ListJobsOptions
}

// PauseResumeOptions governs the behavior
// of the PauseJobs and ResumeJobs methods.
// The eventual fields are combined when querying the
// database.
type PauseResumeOptions struct {
	// JobIDs specifies which jobs will be
	// paused or resumed.
	JobIDs []int64
	// GroupIDs specifies the group ids
	// whose jobs will be paused or resumed.
	GroupIDs []int64
	// SuperGroupIDs specifies the super group ids
	// whose jobs will be paused or resumed.
	SuperGroupIDs []int64
}

// ToListOptions convert to ListJobOptions. Just as in ListJobOptions,
// all fields are combined.
func (o *PauseResumeOptions) toListOptions() ListJobsOptions {
	// otherwise, fill in all the other options.
	return ListJobsOptions{
		Paused:        nil,
		GroupIDs:      o.GroupIDs,
		SuperGroupIDs: o.SuperGroupIDs,
		JobIDs:        o.JobIDs,
	}
}

// PauseResumeOptions governs the behavior
// of the DeleteJobs method.
type DeleteOptions struct {
	PauseResumeOptions
	// Paused specifies whether to delete paused
	// jobs or not (or do not care about it).
	Paused *bool
}

// ToListOptions convert to ListJobOptions by preserving the
// different semantics of the two struct, i.e., on ListJobOptions
// all options can be combined, while here JobIDs is exclusive.
func (o *DeleteOptions) toListOptions() ListJobsOptions {
	if o.JobIDs != nil {
		return ListJobsOptions{
			JobIDs: o.JobIDs,
		}
	}
	// otherwise fill in all the other options except
	// JobIDs.
	return ListJobsOptions{
		Paused:        o.Paused,
		GroupIDs:      o.GroupIDs,
		SuperGroupIDs: o.SuperGroupIDs,
		JobIDs:        nil,
	}
}
