package smallben

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"os"
	"reflect"
	"testing"
)

const KeyTestPgDbName = "TEST_DATABASE_PG"

var (
	// accessed by the TestCronJob
	// indexed by the id of the job
	accessed = make(map[int64]CronJobInput)

	pgConn = os.Getenv(KeyTestPgDbName)
)

func init() {
	gob.Register(&TestCronJob{})
}

// fake struct implementing the CronJob interface
type TestCronJob struct{}

func (t *TestCronJob) Run(input CronJobInput) {
	accessed[input.JobID] = input
}

type RepositoryTestSuite struct {
	repository Repository3
	jobsToAdd  []JobWithSchedule
}

func NewRepositoryTestSuite(dialector gorm.Dialector, t *testing.T) *RepositoryTestSuite {
	r := new(RepositoryTestSuite)
	// dialector := postgres.Open(viper.GetString(KeyTestPgDbName))
	repository, err := NewRepository3(dialector, &gorm.Config{})
	if err != nil {
		t.FailNow()
	}
	r.repository = repository
	return r
}

// TestAddNoError tests that adding a series of jobsToAdd works.
// Checks various way of retrieve the job, including
// the execution of a retrieved job.
func (r *RepositoryTestSuite) TestAddNoError(t *testing.T) {

	// add them
	err := r.repository.AddJobs(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to add jobsToAdd: %s\n", err.Error())
	}

	// retrieve them using GetRawByIds
	rawJobs, err := r.repository.GetRawJobsByIds(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	if err != nil {
		t.Errorf("Cannot get raw jobs from id: %s\n", err.Error())
	}
	if len(rawJobs) != len(r.jobsToAdd) {
		t.Errorf("The number of retrieved test is wrong. Got %d, expected: %d\n", len(rawJobs), len(r.jobsToAdd))
	}
	// and also using GetJobsByIds
	jobsWithSchedule, err := r.repository.GetJobsByIdS(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	if err != nil {
		t.Errorf("Cannot get jobs from id: %s\n", err.Error())
	}
	if len(jobsWithSchedule) != len(r.jobsToAdd) {
		t.Errorf("The number of retrieved test is wrong. Got %d, expected: %d\n", len(jobsWithSchedule), len(r.jobsToAdd))
	}

	// build the ids making sure they match
	gotIds := GetIdsFromJobsList(rawJobs)
	expectedIds := GetIdsFromJobsWithScheduleList(r.jobsToAdd)
	if !reflect.DeepEqual(gotIds, expectedIds) {
		t.Errorf("The retrieved jobs id is wrong. Got\n%+v\nExpected\n%+v\n", gotIds, expectedIds)
	}

	// retrieve them using GetAllJobsToExecute
	jobs, err := r.repository.GetAllJobsToExecute()
	if err != nil {
		t.Errorf("Cannot get the list of jobs to execute: %s\n", err.Error())
	}
	if len(jobs) != len(r.jobsToAdd) {
		t.Errorf("The number of retrieved jobs to execute is wrong. Got %d, expected: %d", len(jobs), len(r.jobsToAdd))
	}

	gotIds = GetIdsFromJobsWithScheduleList(jobs)
	if !reflect.DeepEqual(gotIds, expectedIds) {
		t.Errorf("The expected ids are wrong. Got\n%+v\nExpected\n%+v\n", gotIds, expectedIds)
	}

	// retrieve one of them
	job, err := r.repository.GetJob(r.jobsToAdd[0].job.ID)
	if err != nil {
		t.Errorf("Fail to retrieve single job: %s\n", err.Error())
		t.FailNow()
	}
	// this check won't work because of time difference
	// r.Equal(job.job, r.jobsToAdd[0].job)

	// also, checking that the input has been correctly
	// recovered
	if !reflect.DeepEqual(job.runInput, r.jobsToAdd[0].runInput) {
		t.Errorf("The retrieved job is wrong. Got\n%+v\nExpected\n%+v\n", job.runInput, r.jobsToAdd[0].runInput)
	}
	// and now execute it
	job.run.Run(job.runInput)
	// making sure it has been executed
	if !reflect.DeepEqual(accessed[job.job.ID], r.jobsToAdd[0].runInput) {
		t.Errorf("The job has not been executed correctly. Got\n%+v\nExpected\n%+v\n", accessed[job.job.ID], r.jobsToAdd[0].runInput)
	}
}

func (r *RepositoryTestSuite) TestPauseJobs(t *testing.T) {
	// add the jobs, this way we can pause them
	err := r.repository.AddJobs(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to add jobs: %s\n", err.Error())
		t.FailNow()
	}

	// get the length of jobs to execute.
	// we will use it for later comparison
	jobsToExecuteBefore, err := r.repository.GetAllJobsToExecute()
	if err != nil {
		t.Errorf("Fail to get all jobs to execute before pause: %s\n", err.Error())
		t.FailNow()
	}
	lenOfJobsToExecuteBefore := len(jobsToExecuteBefore)

	// and now we can pause them
	// we need to convert to the raw format
	rawJobs := make([]Job, len(r.jobsToAdd))
	for i, job := range r.jobsToAdd {
		rawJobs[i] = job.job
	}

	// effectively pause them
	err = r.repository.PauseJobs(rawJobs)
	if err != nil {
		t.Errorf("Fail to pause jobs: %s\n", err.Error())
	}

	// now, compute the number of jobs to execute now
	jobsToExecuteAfterPause, err := r.repository.GetAllJobsToExecute()
	if err != nil {
		t.Errorf("Fail to get all jobs to execute after pause: %s\n", err.Error())
		t.FailNow()
	}
	lenOfJobsToExecuteAfterPause := len(jobsToExecuteAfterPause)

	if lenOfJobsToExecuteBefore != lenOfJobsToExecuteAfterPause+len(r.jobsToAdd) {
		t.Errorf("Something went wrong during the pause. Got\n%d\nExpected\n%d\n",
			lenOfJobsToExecuteBefore, lenOfJobsToExecuteAfterPause-len(r.jobsToAdd))
	}

	// now, resume them
	err = r.repository.ResumeJobs(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to resume jobs: %s\n", err.Error())
	}
	// re-grab the number of jobs
	jobsToExecuteAfterResume, err := r.repository.GetAllJobsToExecute()
	if err != nil {
		t.Errorf("Fail to get all jobs to execute after resume")
		t.FailNow()
	}
	lenOfJobsToExecuteAfterResume := len(jobsToExecuteAfterResume)

	if lenOfJobsToExecuteAfterResume != len(r.jobsToAdd) {
		t.Errorf("Something went wrong during resume. Got\n%d\nExpected\n%d\n",
			lenOfJobsToExecuteAfterResume, len(r.jobsToAdd))
	}
}

func (r *RepositoryTestSuite) TestCronId(t *testing.T) {
	// first, add the jobs
	err := r.repository.AddJobs(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to add jobs: %s\n", err.Error())
		t.FailNow()
	}

	counter := int64(10)
	// now, change the cron id
	for i := range r.jobsToAdd {
		r.jobsToAdd[i].job.CronID = counter
		counter += 10
	}

	// now, save
	err = r.repository.SetCronId(r.jobsToAdd)
	if err != nil {
		t.Errorf("Cannot change cron id: %s", err.Error())
		t.FailNow()
	}

	// and retrieve
	jobs, err := r.repository.GetJobsByIdS(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	counter = 0
	for i, job := range jobs {
		if job.job.CronID != counter+10 {
			t.Errorf("Cron id not set. Got: %d Expected: %d\n", job.job.CronID, int64(i+10))
		}
		counter += 10
	}

	newCronID := int64(100)
	newSchedule := int64(100)

	// now we do the same, but we also change the schedule
	for i := range r.jobsToAdd {
		r.jobsToAdd[i].job.CronID = newCronID
		r.jobsToAdd[i].job.EverySecond = newSchedule
	}

	err = r.repository.SetCronIdAndChangeSchedule(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to set cron id and change schedule: %s", err.Error())
	}

	// retrieve and check
	jobs, err = r.repository.GetJobsByIdS(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	for _, job := range jobs {
		if job.job.CronID != newCronID {
			t.Errorf("Cron id not set. Got: %d Expected: %d\n", job.job.CronID, newCronID)
		}
		if job.job.EverySecond != newSchedule {
			t.Errorf("Schedule not changed. Got: %d Expected: %d\n", job.job.EverySecond, newSchedule)
		}
	}
}

func (r *RepositoryTestSuite) TestPauseJobNotExisting(t *testing.T) {
	notExisting := Job{
		ID: 1000,
	}

	// get with just one
	_, err := r.repository.GetJob(1000)
	if err == nil {
		t.Error("GetJob error expected. Should have been not nil.\n")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
	}

	// get with many
	_, err = r.repository.GetJobsByIdS([]int64{10000})
	if err == nil {
		t.Error("GetJobs error expected. Should have been not nil.\n")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
	}

	// get with many -- raw
	_, err = r.repository.GetRawJobsByIds([]int64{10000})
	if err == nil {
		t.Error("GetRawJobs error expected. Should have been not nil.\n")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
	}

	// pause
	err = r.repository.PauseJobs([]Job{notExisting})
	if err == nil {
		t.Error("Pause error expected. Should have been not nil.\n")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
	}

	// resume
	err = r.repository.ResumeJobs([]JobWithSchedule{{job: notExisting}})
	if err == nil {
		t.Error("Resume error expected. Should have been not nil.\n")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
	}

}

func scheduleNeverFail(t *testing.T, seconds int) cron.Schedule {
	res, err := cron.ParseStandard(fmt.Sprintf("@every %ds", seconds))
	if err != nil {
		t.Error("Schedule conversions should not fail")
		t.FailNow()
	}
	return res
}

func (r *RepositoryTestSuite) setup(t *testing.T) {
	r.jobsToAdd = []JobWithSchedule{
		{
			job: Job{
				ID:           1,
				GroupID:      1,
				SuperGroupID: 1,
				EverySecond:  60,
			},
			run: &TestCronJob{},
			runInput: CronJobInput{
				JobID:        1,
				GroupID:      1,
				SuperGroupID: 1,
				OtherInputs: map[string]interface{}{
					"hello": "is there anybody in there?",
				},
			},
			schedule: scheduleNeverFail(t, 30),
		},
		{
			job: Job{
				ID:           2,
				GroupID:      1,
				SuperGroupID: 1,
				EverySecond:  120,
			},
			run: &TestCronJob{},
			runInput: CronJobInput{
				JobID:        2,
				GroupID:      1,
				SuperGroupID: 1,
				OtherInputs: map[string]interface{}{
					"when I was a child": "I had fever",
				},
			},
			schedule: scheduleNeverFail(t, 60),
		},
	}
}

func (r *RepositoryTestSuite) teardown(okNotFound bool, t *testing.T) {
	err := r.repository.DeleteJobsByIds(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	if err != nil {
		if !(okNotFound && errors.Is(err, gorm.ErrRecordNotFound)) {
			t.Errorf("Cannot delete jobs on teardown: %s\n", err.Error())
		}
	}
}

func buildRepositoryTestSuite(t *testing.T) []*RepositoryTestSuite {
	dialectors := []gorm.Dialector{
		postgres.Open(pgConn),
	}
	tests := make([]*RepositoryTestSuite, len(dialectors))
	for i, dialector := range dialectors {
		tests[i] = NewRepositoryTestSuite(dialector, t)
	}
	return tests
}

func TestRepositoryAddTestSuite(t *testing.T) {
	tests := buildRepositoryTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestAddNoError(t)
		test.teardown(false, t)
	}
}

func TestRepositoryPauseResume(t *testing.T) {
	tests := buildRepositoryTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestPauseJobs(t)
		test.teardown(false, t)
	}
}

func TestRepositoryCronId(t *testing.T) {
	tests := buildRepositoryTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestCronId(t)
		test.teardown(false, t)
	}
}

func TestRepositoryError(t *testing.T) {
	tests := buildRepositoryTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestPauseJobNotExisting(t)
		test.teardown(true, t)
	}
}
