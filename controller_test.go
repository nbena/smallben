package smallben

import (
	"encoding/gob"
	"errors"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"reflect"
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
		t.Errorf("The rawJob count has some problems. Got: %d Expected: %d\n", lenOfCronAfterFirstAdd, len(s.jobs))
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

	// now pause the first rawJob
	jobIDToPause := s.jobs[0].ID
	err = s.smallBen.PauseJobs([]int64{jobIDToPause})
	if err != nil {
		t.Errorf("Fail to pause rawJob: %s\n", err.Error())
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
		t.Errorf("Fail to resume rawJob: %s\n", err.Error())
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

func (s *SmallBenTestSuite) TestChangeSchedule(t *testing.T) {

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

	lenOfJobsBefore := len(s.smallBen.scheduler.cron.Entries())

	// list of cron ids
	cronIDSBefore := make([]int64, len(s.jobs))
	// list of schedules
	schedulesBefore := make([]cron.Schedule, len(s.jobs))
	for i, job := range s.smallBen.scheduler.cron.Entries() {
		cronIDSBefore[i] = int64(job.ID)
		schedulesBefore[i] = job.Schedule
	}

	// now change the schedule
	schedule := UpdateSchedule{
		JobID:       s.jobs[0].ID,
		EverySecond: 120,
	}

	err = s.smallBen.UpdateSchedule([]UpdateSchedule{schedule})
	if err != nil {
		t.Errorf("Fail to change schedule: %s\n", err.Error())
	}

	// now check that the schedules have changed
	lenOfJobsAfter := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsBefore != lenOfJobsAfter {
		t.Errorf("Something went wrong when changing the schedule. Got: %d Expected %d\n",
			lenOfJobsAfter, lenOfJobsBefore)
	}

	// build the new list of schedules and ids
	cronIDSAfter := make([]int64, len(s.jobs))
	schedulesAfter := make([]cron.Schedule, len(s.jobs))
	for i, job := range s.smallBen.scheduler.cron.Entries() {
		cronIDSAfter[i] = int64(job.ID)
		schedulesAfter[i] = job.Schedule
	}

	// they should have changed
	if reflect.DeepEqual(cronIDSAfter, cronIDSBefore) {
		t.Errorf("Something went wrong when changing the schedule. Got: %v\n", cronIDSAfter)
	}
	if reflect.DeepEqual(schedulesBefore, schedulesAfter) {
	}
}

func (s *SmallBenTestSuite) TestOther(t *testing.T) {

	// let's start the scheduler.
	err := s.smallBen.Start()
	if err != nil {
		t.Errorf("Cannot even start: %s\n", err.Error())
		t.FailNow()
	}

	// add a buck of jobs
	err = s.smallBen.AddJobs(s.jobs)
	if err != nil {
		t.Errorf("Fail to add jobs: %s\n", err.Error())
		t.FailNow()
	}

	// pause one of them
	err = s.smallBen.PauseJobs([]int64{s.jobs[0].ID})
	if err != nil {
		t.Errorf("Fail to pause jobs: %s\n", err.Error())
		t.FailNow()
	}

	lenOfJobsAfterPause := len(s.smallBen.scheduler.cron.Entries())

	// now, let's stop it.
	s.smallBen.Stop()

	// now, let's start it once again
	// to test the Fill method
	err = s.smallBen.Start()
	if err != nil {
		t.Errorf("Cannot restart: %s\n", err.Error())
		t.FailNow()
	}

	lenOfJobsAfterRestart := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterRestart != lenOfJobsAfterPause {
		t.Errorf("Fill does not work properly. Got: %d Expected: %d\n",
			lenOfJobsAfterRestart, lenOfJobsAfterPause)
	}

	// now, let's start it once again to make sure nothing bad happens
	err = s.smallBen.Start()
	if err != nil {
		t.Errorf("Cannot re-restart: %s\n", err.Error())
		t.FailNow()
	}

	// now, let's add jobs once again, making sure we trigger an error.
	err = s.smallBen.AddJobs(s.jobs)
	if err == nil {
		t.Errorf("AddJobs should have triggered an error but it didn't\n")
	}

	// now, let's delete a job that does not exist.
	err = s.smallBen.DeleteJobs([]int64{10000})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// same for pause
	err = s.smallBen.PauseJobs([]int64{10000})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// same for resume
	err = s.smallBen.ResumeJobs([]int64{10000})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// same for update
	err = s.smallBen.UpdateSchedule([]UpdateSchedule{
		{JobID: 10000,
			EverySecond: 1,
		}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// new, let's require a non-valid schedule
	err = s.smallBen.UpdateSchedule([]UpdateSchedule{
		{JobID: s.jobs[0].ID,
			// it is not validated by cron
			// but by the database constraint
			EverySecond: -10,
		}})
	if err == nil {
		t.Errorf("A wrong schedule has been accepted")
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("The error is of unexpected type: %s\n", err.Error())
	}
}

func (s *SmallBenTestSuite) setup() {
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
		test.setup()
		test.TestAddDelete(t)
		test.teardown(false, t)
	}
}

func TestSmallBenPauseResume(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup()
		test.TestPauseResume(t)
		test.teardown(true, t)
	}
}

func TestSmallBenChangeSchedule(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup()
		test.TestChangeSchedule(t)
		test.teardown(true, t)
	}
}

func TestSmallBenOther(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup()
		test.TestOther(t)
		test.teardown(true, t)
	}
}
