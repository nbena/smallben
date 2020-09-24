package smallben

import (
	"encoding/gob"
	"fmt"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"os"
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
	suite.Suite
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
func (r *RepositoryTestSuite) TestAddNoError() {

	// add them
	err := r.repository.AddJobs(r.jobsToAdd)
	r.Nil(err, "Cannot add jobsToAdd")

	// retrieve them using GetRawByIds
	rawJobs, err := r.repository.GetRawJobsByIds(GetIdsFromJobsWithScheduleList(r.jobsToAdd))
	r.Nil(err, "Cannot get raw jobsToAdd")
	r.Equal(len(rawJobs), len(r.jobsToAdd))

	// build the ids making sure they match
	gotIds := GetIdsFromJobsList(rawJobs)
	expectedIds := GetIdsFromJobsWithScheduleList(r.jobsToAdd)
	r.Equal(gotIds, expectedIds)

	// retrieve them using GetAllJobsToExecute
	jobs, err := r.repository.GetAllJobsToExecute()
	r.Nil(err, "Cannot get jobsToAdd to execute")
	r.Equal(len(jobs), len(r.jobsToAdd))

	gotIds = GetIdsFromJobsWithScheduleList(jobs)
	r.Equal(gotIds, expectedIds)

	// retrieve one of them
	job, err := r.repository.GetJob(r.jobsToAdd[0].job.ID)
	r.Nil(err, "Cannot get single job")
	// this check won't work because of time difference
	// r.Equal(job.job, r.jobsToAdd[0].job)

	// also, checking that the input has been correctly
	// recovered
	r.Equal(job.runInput, r.jobsToAdd[0].runInput)
	// and now execute it
	job.run.Run(job.runInput)
	// making sure it has been executed
	r.Equal(accessed[job.job.ID], r.jobsToAdd[0].runInput)
}

func scheduleNeverFail(t *testing.T, seconds int) cron.Schedule {
	res, err := cron.ParseStandard(fmt.Sprintf("@every %ds", seconds))
	if err != nil {
		t.Error("Schedule conversions should not fail")
		t.FailNow()
	}
	return res
}

func (r *RepositoryTestSuite) SetupTest() {
	//var buffer bytes.Buffer
	//
	//// prepare some encoded data
	//// they are needed for ToJobWithSchedule
	//// which attempts to
	//encoder := gob.NewEncoder(&buffer)
	//err := encoder.Encode(&TestCronJob{})
	//r.Nil(err)
	//if err != nil {
	//	r.FailNow("Cannot encode fake test")
	//}
	//
	//// encode a simple job input for the first job
	//input_1 := CronJobInput{
	//	JobID:        0,
	//	GroupID:      0,
	//	SuperGroupID: 0,
	//	OtherInputs:  nil,
	//}
	//
	//jobs := []Job {
	//	{
	//		ID: 1,
	//		EverySecond: 10,
	//		GroupID: 1,
	//		SuperGroupID: 2,
	//		SerializedJob: buffer.Bytes(),
	//	},{
	//		ID: 2,
	//		EverySecond: 30,
	//		GroupID: 1,
	//		SuperGroupID: 2,
	//		SerializedJob: buffer.Bytes(),
	//	},
	//}
	//for _, rawJob := range jobs {
	//	job, err := rawJob.ToJobWithSchedule()
	//	r.NotNil(err, "Cannot build test")
	//	// should never happen
	//	if err != nil {
	//		r.FailNow("Cannot set up test with error: %s\n", err.Error())
	//	}
	//	r.jobsToAdd = append(r.jobsToAdd, job)
	//}
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
			schedule: scheduleNeverFail(r.Suite.T(), 30),
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
			schedule: scheduleNeverFail(r.Suite.T(), 60),
		},
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
		suite.Run(t, test)
	}
}
