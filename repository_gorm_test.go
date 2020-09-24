package smallben

import (
	"encoding/gob"
	"github.com/stretchr/testify/suite"
)

func init() {
	gob.Register(TestCronJob{})
}

// accessed by the TestCronJob
// indexed by the id of the job
var accessed map[int64]CronJobInput

// fake struct implementing the CronJob interface
type TestCronJob struct{}

func (t *TestCronJob) Run(input CronJobInput) {
	accessed[input.JobID] = input
}

type RepositoryTestSuite struct {
	suite.Suite
	repository Repository3
	jobs       []JobWithSchedule
}

// TestAddNoError tests that adding a series of jobs works.
// Checks various way of retrieve the job, including
// the execution of a retrieved job.
func (r *RepositoryTestSuite) TestAddNoError() {
	// add them
	err := r.repository.AddJobs(r.jobs)
	r.Nil(err, "Cannot add jobs")

	// retrieve them using GetRawByIds
	rawJobs, err := r.repository.GetRawJobsByIds(GetIdsFromJobsWithScheduleList(r.jobs))
	r.Nil(err, "Cannot get raw jobs")
	r.Equal(len(rawJobs), len(r.jobs))

	// build the ids making sure they match
	gotIds := GetIdsFromJobsList(rawJobs)
	expectedIds := GetIdsFromJobsWithScheduleList(r.jobs)
	r.Equal(gotIds, expectedIds)

	// retrieve them using GetAllJobsToExecute
	jobs, err := r.repository.GetAllJobsToExecute()
	r.Nil(err, "Cannot get jobs to execute")
	r.Equal(len(jobs), len(r.jobs))

	gotIds = GetIdsFromJobsWithScheduleList(jobs)
	r.Equal(gotIds, expectedIds)

	// retrieve one of them
	job, err := r.repository.GetJob(r.jobs[0].job.ID)
	r.Nil(err, "Cannot get single job")
	r.Equal(job.job, r.jobs[0].job)

	// also, checking that the input has been correctly
	// recovered
	r.Equal(job.runInput, r.jobs[0].runInput)
	// and now execute it
	job.run.Run(job.runInput)
	// making sure it has been executed
	r.Equal(accessed[job.job.ID], r.jobs[0].runInput)
}
