package smallben

import (
	"encoding/gob"
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
		t.Errorf("Cannot get raw jobsToAdd: %s\n", err.Error())
	}
	if len(rawJobs) != len(r.jobsToAdd) {
		t.Errorf("The number of retrieved test is wrong. Got %d, expected: %d\n", len(rawJobs), len(r.jobsToAdd))
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

func (r *RepositoryTestSuite) teardown(t *testing.T) {
	err := r.repository.DeleteJobsByIds(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	if err != nil {
		t.Errorf("Cannot delete jobs on teardown: %s\n", err.Error())
	}
}

func TestRepositoryTestSuite(t *testing.T) {
	dialectors := []gorm.Dialector{
		postgres.Open(pgConn),
	}
	tests := make([]*RepositoryTestSuite, len(dialectors))
	for i, dialector := range dialectors {
		tests[i] = NewRepositoryTestSuite(dialector, t)
	}

	for _, test := range tests {
		test.setup(t)
		test.TestAddNoError(t)
		test.teardown(t)
	}
}
