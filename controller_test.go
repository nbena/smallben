package smallben

import (
	"encoding/gob"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"reflect"
	"sync"
	"testing"
)

type SmallBenTestSuite struct {
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
	jobs, err := s.smallBen.repository.ListJobs(&ListJobsOptions{JobIDs: GetIdsFromJobList(s.jobs)})
	if err != nil {
		t.Errorf("Fail to get jobs from the db: %s", err.Error())
	}
	if len(jobs) != len(s.jobs) {
		t.Errorf("Some jobs have not been added to the db. Got: %d Expected %d\n", len(jobs), len(s.jobs))
	}

	// and directly using the method from the controller
	jobsFromController, err := s.smallBen.ListJobs(&ListJobsOptions{JobIDs: GetIdsFromJobList(s.jobs)})
	if err != nil {
		t.Errorf("Fail to get jobs from the controller: %s\n", err.Error())
	}
	if len(jobsFromController) != len(s.jobs) {
		t.Errorf("Some jobs have not been added. Got: %d Expected %d\n", len(jobsFromController), len(s.jobs))
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
	// using the JobID filter.
	jobIDToPause := s.jobs[0].ID
	err = s.smallBen.PauseJobs(&PauseResumeOptions{JobIDs: []int64{jobIDToPause}})
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
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{
		JobIDs: []int64{jobIDToPause},
	})
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

	// now, let's pause all the jobs staying in the first available group
	group := s.jobs[0].GroupID
	var jobsOfGroup []int64
	for _, job := range s.jobs {
		if job.GroupID == group {
			jobsOfGroup = append(jobsOfGroup, job.ID)
		}
	}
	err = s.smallBen.PauseJobs(&PauseResumeOptions{
		GroupIDs: []int64{group},
	})
	if err != nil {
		t.Errorf("Fail to pause jobs by group id: %s\n", err.Error())
		t.FailNow()
	}
	// now, check the number of entries
	lenOfJobsAfterSecondPause := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterSecondPause != len(s.jobs)-len(jobsOfGroup) {
		t.Errorf("Something went wrong after pause. Got: %d Expected: %d\n",
			lenOfJobsAfterSecondPause, len(s.jobs)-len(jobsOfGroup))
	}

	// now, resume them using the same options
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{GroupIDs: []int64{group}})
	if err != nil {
		t.Errorf("Fail to resume jobs by group id: %s\n", err.Error())
		t.FailNow()
	}
	// and check the number of entries
	lenOfJobsAfterSecondResume := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterSecondResume != len(s.jobs) {
		t.Errorf("Something went wrong after resume. Got: %d Expected: %d\n",
			lenOfJobsAfterSecondResume, len(s.jobs))
	}

	// now do the same but by using supergroup
	// and the group
	superGroup := s.jobs[0].SuperGroupID
	var jobsOfSuperGroupAndGroup []int64
	for _, job := range s.jobs {
		if job.GroupID == group && job.SuperGroupID == superGroup {
			jobsOfSuperGroupAndGroup = append(jobsOfSuperGroupAndGroup, job.ID)
		}
	}
	err = s.smallBen.PauseJobs(&PauseResumeOptions{
		GroupIDs:      []int64{group},
		SuperGroupIDs: []int64{superGroup},
	})
	if err != nil {
		t.Errorf("Fail to pause jobs by group id and super group id: %s\n", err.Error())
		t.FailNow()
	}
	// now, check the number of entries
	lenOfJobsAfterThirdPause := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterThirdPause != len(s.jobs)-len(jobsOfSuperGroupAndGroup) {
		t.Errorf("Something went wrong after pause. Got: %d Expected: %d\n",
			lenOfJobsAfterThirdPause, len(s.jobs)-len(jobsOfSuperGroupAndGroup))
	}

	// now, resume them using the same options
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{
		GroupIDs:      []int64{group},
		SuperGroupIDs: []int64{superGroup},
	})
	if err != nil {
		t.Errorf("Fail to resume jobs by group id and super group id: %s\n", err.Error())
		t.FailNow()
	}
	// and check the number of entries
	lenOfJobsAfterThirdResume := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterThirdResume != len(s.jobs) {
		t.Errorf("Something went wrong after resume. Got: %d Expected: %d\n",
			lenOfJobsAfterThirdResume, len(s.jobs))
	}

	// now, do the same, but by super group only
	var jobsOfSuperGroup []int64
	for _, job := range s.jobs {
		if job.SuperGroupID == superGroup {
			jobsOfSuperGroup = append(jobsOfSuperGroup, job.ID)
		}
	}
	err = s.smallBen.PauseJobs(&PauseResumeOptions{
		SuperGroupIDs: []int64{superGroup},
	})
	if err != nil {
		t.Errorf("Fail to pause jobs by super group id: %s\n", err.Error())
		t.FailNow()
	}
	// now, check the number of entries
	lenOfJobsAfterFourthPause := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterFourthPause != len(s.jobs)-len(jobsOfSuperGroup) {
		t.Errorf("Something went wrong after pause. Got: %d Expected: %d\n",
			lenOfJobsAfterFourthPause, len(s.jobs)-len(jobsOfSuperGroup))
	}

	// now, resume them using the same options
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{
		SuperGroupIDs: []int64{superGroup},
	})
	if err != nil {
		t.Errorf("Fail to resume jobs by super group id: %s\n", err.Error())
		t.FailNow()
	}
	// and check the number of entries
	lenOfJobsAfterFourthResume := len(s.smallBen.scheduler.cron.Entries())
	if lenOfJobsAfterFourthResume != len(s.jobs) {
		t.Errorf("Something went wrong after resume. Got: %d Expected: %d\n",
			lenOfJobsAfterFourthResume, len(s.jobs))
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
		JobID:          s.jobs[0].ID,
		CronExpression: "@every 120s",
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
	err = s.smallBen.PauseJobs(&PauseResumeOptions{JobIDs: []int64{s.jobs[0].ID}})
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
}

func (s *SmallBenTestSuite) TestErrors(t *testing.T) {

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

	// now, let's add jobs once again, making sure we trigger an error.
	err = s.smallBen.AddJobs(s.jobs)
	if err == nil {
		t.Errorf("AddJobs should have triggered an error but it didn't\n")
	}

	// now, let's delete a job that does not exist.
	err = s.smallBen.DeleteJobs(&DeleteOptions{PauseResumeOptions: PauseResumeOptions{
		JobIDs: []int64{10000},
	}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// same for pause
	err = s.smallBen.PauseJobs(&PauseResumeOptions{JobIDs: []int64{10000}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// now, let's check all the wrong combination of
	// the PauseResumeOptions
	// all nil
	err = s.smallBen.PauseJobs(&PauseResumeOptions{})
	checkErrorIsOf(err, ErrPauseResumeOptionsBad, "ErrPauseResumeOptionsBad", t)
	// jobIDs but also other fields
	err = s.smallBen.PauseJobs(&PauseResumeOptions{
		JobIDs:   []int64{1000},
		GroupIDs: []int64{1000},
	})
	checkErrorIsOf(err, ErrPauseResumeOptionsBad, "ErrPauseResumeOptionsBad", t)

	// same for resume
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{
		JobIDs: []int64{1000},
	})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)
	// now, let's check all the wrong combination of
	// the PauseResumeOptions
	// all nil
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{})
	checkErrorIsOf(err, ErrPauseResumeOptionsBad, "ErrPauseResumeOptionsBad", t)
	// jobIDs but also other fields
	err = s.smallBen.ResumeJobs(&PauseResumeOptions{
		JobIDs:   []int64{1000},
		GroupIDs: []int64{1000},
	})
	checkErrorIsOf(err, ErrPauseResumeOptionsBad, "ErrPauseResumeOptionsBad", t)

	// same for update
	err = s.smallBen.UpdateSchedule([]UpdateSchedule{
		{JobID: 10000,
			CronExpression: "@every 1s",
		}})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "gorm.ErrRecordNotFound", t)

	// let's do a delete with a bad options
	err = s.smallBen.DeleteJobs(&DeleteOptions{
		PauseResumeOptions: PauseResumeOptions{
			JobIDs:   []int64{1000},
			GroupIDs: []int64{1000},
		},
	})
	checkErrorIsOf(err, ErrPauseResumeOptionsBad, "ErrPauseResumeOptionsBad", t)

	// new, let's require a non-valid schedule
	err = s.smallBen.UpdateSchedule([]UpdateSchedule{
		{JobID: s.jobs[0].ID,
			CronExpression: "not valid expression",
		}})
	if err == nil {
		t.Errorf("A wrong schedule has been accepted")
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("The error is of unexpected type: %s\n", err.Error())
	}

	// now we even test a wrong connection.
	wrongConfig := RepositoryGormConfig{
		Dialector: postgres.Open(""),
		Config:    gorm.Config{},
	}

	_, err = NewRepositoryGorm(&wrongConfig)
	if err == nil {
		t.Errorf("An empty connection has been accepted")
	}

}

func (s *SmallBenTestSuite) setup(t *testing.T) {
	if err := s.smallBen.RegisterMetrics(prometheus.NewRegistry()); err != nil {
		t.Errorf("Fail to register metrics: %s\n", err.Error())
		t.FailNow()
	}
	s.jobs = JobsToUse
}

func (s *SmallBenTestSuite) teardown(okNotFound bool, t *testing.T) {
	err := s.smallBen.DeleteJobs(
		&DeleteOptions{
			PauseResumeOptions: PauseResumeOptions{
				JobIDs: GetIdsFromJobList(s.jobs),
			},
		})
	// GetIdsFromJobList(s.jobs))
	if err != nil {
		if !(okNotFound && errors.Is(err, gorm.ErrRecordNotFound)) {
			t.Errorf("Fail to delete: %s", err.Error())
		}
	}
	s.smallBen.Stop()
}

func newGormRepository(config *RepositoryGormConfig, t *testing.T) Repository {
	repository, err := NewRepositoryGorm(config)
	if err != nil {
		t.Errorf("Cannot open connection: %s\n", err.Error())
		t.FailNow()
	}
	return repository
}

// Builds the list of test suites to execute.
func buildSmallBenTestSuite(t *testing.T) []*SmallBenTestSuite {

	repositories := []Repository{
		newGormRepository(&RepositoryGormConfig{Dialector: postgres.Open(pgConn)}, t),
	}
	tests := make([]*SmallBenTestSuite, len(repositories))

	for i, repository := range repositories {
		tests[i] = &SmallBenTestSuite{smallBen: New(repository, cron.WithSeconds())}
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

func TestSmallBenChangeSchedule(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestChangeSchedule(t)
		test.teardown(true, t)
	}
}

func TestSmallBenOther(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestOther(t)
		test.teardown(true, t)
	}
}

func TestSmallBenError(t *testing.T) {
	tests := buildSmallBenTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestErrors(t)
		test.teardown(true, t)
	}
}

var JobsToUse = []Job{
	{
		ID:             1,
		GroupID:        1,
		SuperGroupID:   1,
		CronExpression: "@every 70s",
		Job:            &SmallBenCronJob{},
		JobInput: map[string]interface{}{
			"test_id": 1,
		},
	}, {
		ID:             2,
		GroupID:        1,
		SuperGroupID:   1,
		CronExpression: "@every 62s",
		Job:            &SmallBenCronJob{},
		JobInput: map[string]interface{}{
			"test_id": 2,
		},
	}, {
		ID:             3,
		GroupID:        2,
		SuperGroupID:   1,
		CronExpression: "@every 61s",
		Job:            &SmallBenCronJob{},
		JobInput: map[string]interface{}{
			"test_id": 3,
		},
	}, {
		ID:             4,
		GroupID:        2,
		SuperGroupID:   2,
		CronExpression: "@every 60s",
		Job:            &SmallBenCronJob{},
		JobInput: map[string]interface{}{
			"test_id": 4,
		},
	},
}
