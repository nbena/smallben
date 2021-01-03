package smallben

// Repository is the interface whose storage backends should implement.
type Repository interface {
	// AddJobs adds `jobs` to the backend. This operation
	// must be atomic.
	AddJobs(jobs []JobWithSchedule) error
	// GetJob returns the job whose ID is `jobID`, returning
	// an error in case the required job has not been found.
	// That error must be of the type returned by ErrorTypeIfMismatchCount().
	GetJob(jobID int64) (JobWithSchedule, error)
	// PauseJobs pause `jobs`, i.e., marks them as paused.
	// This operation must be atomic.
	//
	// It must return an error of type ErrorTypeIfMismatchCount() in case
	// the number of updated jobs is different than the number
	// of required jobs, i.e., `len(jobs)`.
	// It should update all the jobs it can, i.e., not stopping
	// at the first ID not found.
	PauseJobs(jobs []RawJob) error
	// ResumeJobs resumes `jobs`, i.e., marks them as non-paused.
	// This operation must be atomic.
	//
	// It must return an error of type ErrorTypeIfMismatchCount() in case
	// the number of updated jobs is different than the number
	// of required jobs, i.e., `len(jobs)`.
	// It should update all the jobs it can, i.e., not stopping
	// at the first ID not found.
	ResumeJobs(jobs []JobWithSchedule) error
	// GetAllJobsToExecute returns all jobs the scheduler
	// should execute on startup, i.e., all jobs whose `paused` field
	// is set to `false`.
	GetAllJobsToExecute() ([]JobWithSchedule, error)
	// GetJobsByIds returns an array of jobs whose id is in `jobsID`.
	//
	// It must return an error of type ErrorTypeIfMismatchCount() in case
	// the number of returned jobs is different than the number
	// of required jobs, i.e., `len(jobsID)`.
	GetJobsByIds(jobsID []int64) ([]JobWithSchedule, error)
	// DeleteJobsByIds deletes all the jobs whose id is in `jobsID`.
	//
	// It must return an error of type ErrorTypeIfMismatchCount() in case
	// the number of deleted jobs is different than the number
	// of required jobs, i.e., `len(jobsID)`.
	DeleteJobsByIds(jobsID []int64) error
	// SetCronId updates the `cron_id` field of `jobs`.
	//
	// It must return an error of type ErrorTypeIfMismatchCount() in case
	// the number of updated jobs is different than the number
	// of required jobs, i.e., `len(jobs)`.
	SetCronId(jobs []JobWithSchedule) error
	// SetCronIdAndChangeSchedule updates the `cron_id`, `cron_expression`
	// and `job_input` fields of RawJob.
	//
	// It must return an error of type ErrorType() in case
	// the number of updated jobs is different than the number
	// of required jobs, i.e., `len(jobs)`.
	SetCronIdAndChangeScheduleAndJobInput(jobs []JobWithSchedule) error
	// ListJobs list all the jobs present in the job storage backend,
	// according to `options`.
	//
	// If options is `nil`, no filtering is applied.
	ListJobs(options ToListOptions) ([]RawJob, error)
	// ErrorTypeIfMismatchCount specifies the error type
	// to return in case there is a mismatch count
	// between the number of jobs involved in a backend operation
	// and the number of supposed jobs that should have been involved
	// in such an operation.
	ErrorTypeIfMismatchCount() error
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

// Need to implement the ToListOptions interface.
func (o *ListJobsOptions) toListOptions() ListJobsOptions {
	return *o
}
