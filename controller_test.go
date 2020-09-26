package smallben

import (
	"encoding/gob"
	"errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"sync"
	"testing"
)

type SmallBenTestSuite struct {
	config   Config
	smallBen *SmallBen
	jobs     []Job
}

type TestMap struct {
	data map[int64]int
	lock sync.Mutex
}

var (
	testMap = new(TestMap)
)

type SmallBenCronJob struct{}

func (s *SmallBenCronJob) Run(input CronJobInput) {
	testMap.lock.Lock()
	defer testMap.lock.Unlock()
	testMap.data[input.JobID] = input.OtherInputs["test_id"].(int)
}

func init() {
	defer func() {
		recover()
	}()
	gob.Register(&SmallBenCronJob{})
}

func (s *SmallBenTestSuite) newSmallBen(t *testing.T) *SmallBen {
	smallBen, err := NewSmallBen(&s.config)
	if err != nil {
		t.Errorf("Cannot create the SmallBen")
		t.FailNow()
	}
	return smallBen
}

func NewSmallBenTestSuite(dialector gorm.Dialector, t *testing.T) *SmallBenTestSuite {
	s := new(SmallBenTestSuite)
	config := Config{
		DbDialector: dialector,
		DbConfig:    gorm.Config{},
	}
	s.config = config

	s.smallBen = s.newSmallBen(t)
	return s
}

func (s *SmallBenTestSuite) TestAddDelete(t *testing.T) {

	// first and foremost, let's start the
	// SmallBen
	err := s.smallBen.Start()
	if err != nil {
		t.Errorf("Cannot even start: %s\n", err.Error())
		t.FailNow()
	}

	// ok, since the database is empty
	// let's make sure of it.
	lenOfCronBeforeAnything := len(s.smallBen.scheduler.cron.Entries())
	if lenOfCronBeforeAnything != 0 {
		t.Errorf("These jobs should have not been there: %d\n", lenOfCronBeforeAnything)
		t.FailNow()
	}

	// let's add the jobs
	err = s.smallBen.AddJobs(s.jobs)
	if err != nil {
		t.Errorf("Fail to add jobs: %s\n", err.Error())
		t.FailNow()
	}

	// do they have been added?
	lenOfCronAfterFirstAdd := len(s.smallBen.scheduler.cron.Entries())
	if lenOfCronAfterFirstAdd != len(s.jobs) {
		t.Errorf("The job count has some problems. Got: %d Expected: %d\n", lenOfCronAfterFirstAdd, len(s.jobs))
	}

	// let's also retrieve it from the database
	jobs, err := s.smallBen.repository.GetRawJobsByIds(GetIdsFromJobList(s.jobs))
	if err != nil {
		t.Errorf("Fail to get jobs from the db: %s", err.Error())
	}
	if len(jobs) != len(s.jobs) {
		t.Errorf("Some jobs have not been added to the db. Got: %d Expected %d\n", len(jobs), len(s.jobs))
	}
}

func (s *SmallBenTestSuite) TestPauseResume(t *testing.T) {

	// first and foremost, let's start the
	// SmallBen
	err := s.smallBen.Start()
	if err != nil {
		t.Errorf("Cannot even start: %s\n", err.Error())
		t.FailNow()
	}

	// add the jobs
	// let's add the jobs
	err = s.smallBen.AddJobs(s.jobs)
	if err != nil {
		t.Errorf("Fail to add jobs: %s\n", err.Error())
		t.FailNow()
	}

	lenOfJobsBeforeAnything := len(s.smallBen.scheduler.cron.Entries())

	// now pause the first job
	jobIDToPause := s.jobs[0].ID
	err = s.smallBen.PauseJobs([]int64{jobIDToPause})
	if err != nil {
		t.Errorf("Fail to pause job: %s\n", err.Error())
		t.FailNow()
	}

	// make sure the number of entries have been decreased by one
	lenOfJobsAfterFirstPause := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsBeforeAnything != lenOfJobsAfterFirstPause+1 {
		t.Errorf("Something went wrong after pause. Got: %d Expected: %d",
			lenOfJobsAfterFirstPause, lenOfJobsAfterFirstPause-1)
	}

	// now, resume it.
	err = s.smallBen.ResumeJobs([]int64{jobIDToPause})
	if err != nil {
		t.Errorf("Fail to resume job: %s\n", err.Error())
		t.FailNow()
	}

	// check that the entries have been increased by one
	// matching the old number of entries
	lenOfJobsAfterResume := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterResume != lenOfJobsBeforeAnything || lenOfJobsAfterResume != lenOfJobsAfterFirstPause+1 {
		t.Errorf("Something went wrong after resume. Got %d: Expected :%d",
			lenOfJobsAfterResume, lenOfJobsBeforeAnything)
	}
}

func (s *SmallBenTestSuite) setup(t *testing.T) {
	jobs := []Job{
		{
			ID:           1,
			GroupID:      1,
			SuperGroupID: 1,
			EverySecond:  70,
			Job:          &SmallBenCronJob{},
			JobInput: map[string]interface{}{
				"test_id": 1,
			},
		}, {
			ID:           2,
			GroupID:      1,
			SuperGroupID: 1,
			EverySecond:  62,
			Job:          &SmallBenCronJob{},
			JobInput: map[string]interface{}{
				"test_id": 2,
			},
		}, {
			ID:           3,
			GroupID:      2,
			SuperGroupID: 1,
			EverySecond:  61,
			Job:          &SmallBenCronJob{},
			JobInput: map[string]interface{}{
				"test_id": 3,
			},
		}, {
			ID:           4,
			GroupID:      2,
			SuperGroupID: 2,
			EverySecond:  60,
			Job:          &SmallBenCronJob{},
			JobInput: map[string]interface{}{
				"test_id": 4,
			},
		},
	}
	s.jobs = jobs
}

func (s *SmallBenTestSuite) teardown(okNotFound bool, t *testing.T) {
	err := s.smallBen.DeleteJobs(GetIdsFromJobList(s.jobs))
	if err != nil {
		if !(okNotFound && errors.Is(err, gorm.ErrRecordNotFound)) {
			t.Errorf("Fail to delete: %s", err.Error())
		}
	}
	s.smallBen.Stop()
}

func buildSmallBenTestSuite(t *testing.T) []*SmallBenTestSuite {
	dialectors := []gorm.Dialector{
		postgres.Open(pgConn),
	}
	tests := make([]*SmallBenTestSuite, len(dialectors))
	for i, dialector := range dialectors {
		tests[i] = NewSmallBenTestSuite(dialector, t)
	}
	return tests
}

func TestSmallBenAdd(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestAddDelete(t)
		test.teardown(false, t)
	}
}

func TestSmallBenPauseResume(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestPauseResume(t)
		test.teardown(true, t)
	}
}
