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
	// indexed by the id of the rawJob
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
// Checks various way of retrieve the rawJob, including
// the execution of a retrieved rawJob.
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
	gotIds := GetIdsFromJobRawList(rawJobs)
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
	job, err := r.repository.GetJob(r.jobsToAdd[0].rawJob.ID)
	if err != nil {
		t.Errorf("Fail to retrieve single rawJob: %s\n", err.Error())
		t.FailNow()
	}
	// this check won't work because of time difference
	// r.Equal(rawJob.rawJob, r.jobsToAdd[0].rawJob)

	// also, checking that the input has been correctly
	// recovered
	if !reflect.DeepEqual(job.runInput, r.jobsToAdd[0].runInput) {
		t.Errorf("The retrieved rawJob is wrong. Got\n%+v\nExpected\n%+v\n", job.runInput, r.jobsToAdd[0].runInput)
	}
	// and now execute it
	job.run.Run(job.runInput)
	// making sure it has been executed
	if !reflect.DeepEqual(accessed[job.rawJob.ID], r.jobsToAdd[0].runInput) {
		t.Errorf("The rawJob has not been executed correctly. Got\n%+v\nExpected\n%+v\n", accessed[job.rawJob.ID], r.jobsToAdd[0].runInput)
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
	rawJobs := make([]RawJob, len(r.jobsToAdd))
	for i, job := range r.jobsToAdd {
		rawJobs[i] = job.rawJob
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
		t.Errorf("Fail to get all jobs to execute after resume: %s\n", err.Error())
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
		r.jobsToAdd[i].rawJob.CronID = counter
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
		if job.rawJob.CronID != counter+10 {
			t.Errorf("Cron id not set. Got: %d Expected: %d\n", job.rawJob.CronID, int64(i+10))
		}
		counter += 10
	}

	newCronID := int64(100)
	newSchedule := int64(100)

	// now we do the same, but we also change the schedule
	for i := range r.jobsToAdd {
		r.jobsToAdd[i].rawJob.CronID = newCronID
		r.jobsToAdd[i].rawJob.EverySecond = newSchedule
	}

	err = r.repository.SetCronIdAndChangeSchedule(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to set cron id and change schedule: %s", err.Error())
	}

	// retrieve and check
	jobs, err = r.repository.GetJobsByIdS(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	for _, job := range jobs {
		if job.rawJob.CronID != newCronID {
			t.Errorf("Cron id not set. Got: %d Expected: %d\n", job.rawJob.CronID, newCronID)
		}
		if job.rawJob.EverySecond != newSchedule {
			t.Errorf("Schedule not changed. Got: %d Expected: %d\n", job.rawJob.EverySecond, newSchedule)
		}
	}
}

func checkErrorIsOf(err, expected error, msg string, t *testing.T) {
	if err == nil {
		t.Errorf("%s error expected. Should have been not nil.\n", msg)
	}
	if !errors.Is(err, expected) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
	}
}

func (r *RepositoryTestSuite) TestPauseJobNotExisting(t *testing.T) {
	notExisting := RawJob{
		ID:     1000,
		CronID: 10000,
	}

	// get with just one
	_, err := r.repository.GetJob(1000)
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "GetJob", t)

	// get with many
	_, err = r.repository.GetJobsByIdS([]int64{10000})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "GetJobs", t)

	// get with many -- raw
	_, err = r.repository.GetRawJobsByIds([]int64{10000})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "GetRawJobs", t)

	// pause
	err = r.repository.PauseJobs([]RawJob{notExisting})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "Pause", t)

	// resume
	err = r.repository.ResumeJobs([]JobWithSchedule{{rawJob: notExisting}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "Resume", t)

	// set cron id
	err = r.repository.SetCronId([]JobWithSchedule{{rawJob: notExisting}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "SetCronId", t)

	// set cron id and change schedule
	err = r.repository.SetCronIdAndChangeSchedule([]JobWithSchedule{{rawJob: notExisting}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "SetCronIdAndChangeSchedule", t)

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
			rawJob: RawJob{
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
			rawJob: RawJob{
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
