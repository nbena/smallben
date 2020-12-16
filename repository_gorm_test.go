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
	"strings"
	"testing"
)

const KeyTestPgDbName = "TEST_DATABASE_PG"

var (
	// accessed by the TestCronJobNoop
	// indexed by the id of the rawJob
	accessed = make(map[int64]CronJobInput)

	pgConn = os.Getenv(KeyTestPgDbName)
)

func init() {
	gob.Register(&TestCronJobNoop{})
	gob.Register(&TestCronJobModifyMap{})
}

// fake struct implementing the CronJob interface
// it just writes its input into the accessed map
type TestCronJobNoop struct{}

func (t *TestCronJobNoop) Run(input CronJobInput) {
	accessed[input.JobID] = input
}

// fake struct implementing the CronJob interface
// it writes its input into the accessed map
// but the value at input.JobId is the sum
// of JobID, GroupID and SuperGroupID
type TestCronJobModifyMap struct{}

func (t *TestCronJobModifyMap) Run(input CronJobInput) {
	input.JobID = input.JobID + input.GroupID + input.SuperGroupID
	accessed[input.JobID] = input
}

type RepositoryTestSuite struct {
	repository Repository
	jobsToAdd  []JobWithSchedule
}

func NewRepositoryTestSuite(dialector gorm.Dialector, t *testing.T) *RepositoryTestSuite {
	r := new(RepositoryTestSuite)
	// dialector := postgres.Open(viper.GetString(KeyTestPgDbName))
	repository, err := NewRepositoryGorm(&RepositoryGormConfig{Dialector: dialector, Config: gorm.Config{}})
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
	rawJobs, err := r.repository.ListJobs(&ListJobsOptions{JobIDs: getIdsFromJobsWithScheduleList(r.jobsToAdd)})
	if err != nil {
		t.Errorf("Cannot get raw jobs from id: %s\n", err.Error())
	}
	if len(rawJobs) != len(r.jobsToAdd) {
		t.Errorf("The number of retrieved test is wrong. Got %d, expected: %d\n", len(rawJobs), len(r.jobsToAdd))
	}
	// and also using GetJobsByIds
	jobsWithSchedule, err := r.repository.GetJobsByIds(getIdsFromJobsWithScheduleList(r.jobsToAdd))
	if err != nil {
		t.Errorf("Cannot get jobs from id: %s\n", err.Error())
	}
	if len(jobsWithSchedule) != len(r.jobsToAdd) {
		t.Errorf("The number of retrieved test is wrong. Got %d, expected: %d\n", len(jobsWithSchedule), len(r.jobsToAdd))
	}

	// build the ids making sure they match
	gotIds := getIdsFromJobRawList(rawJobs)
	expectedIds := getIdsFromJobsWithScheduleList(r.jobsToAdd)
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

	gotIds = getIdsFromJobsWithScheduleList(jobs)
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
	// we have to do some type assertions to make the proper check
	// since TestCronJobNoop just places its values
	// but TestCronJobModifyMap sets another value for job id
	var expectedInput CronJobInput
	if reflect.TypeOf(job.run) == reflect.TypeOf(&TestCronJobNoop{}) {
		expectedInput = r.jobsToAdd[0].runInput
	} else if reflect.TypeOf(job.run) == reflect.TypeOf(&TestCronJobModifyMap{}) {
		expectedInput = r.jobsToAdd[0].runInput
		expectedInput.JobID = expectedInput.JobID + expectedInput.GroupID + expectedInput.SuperGroupID
	} else {
		t.Errorf("Cannot do type interference. Type is: %s\n", reflect.TypeOf(job.run).String())
		t.FailNow()
	}
	// now, let's compare
	if !reflect.DeepEqual(accessed[job.rawJob.ID], expectedInput) {
		t.Errorf("The rawJob has not been executed correctly. Got\n%+v\nExpected\n%+v\n", accessed[job.rawJob.ID], expectedInput)
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
	jobs, err := r.repository.GetJobsByIds(getIdsFromJobsWithScheduleList(r.jobsToAdd))
	counter = 0
	for i, job := range jobs {
		if job.rawJob.CronID != counter+10 {
			t.Errorf("Cron id not set. Got: %d Expected: %d\n", job.rawJob.CronID, int64(i+10))
		}
		counter += 10
	}

	newCronID := int64(100)
	newSchedule := "@every 100s"

	// now we do the same, but we also change the schedule
	for i := range r.jobsToAdd {
		r.jobsToAdd[i].rawJob.CronID = newCronID
		r.jobsToAdd[i].rawJob.CronExpression = newSchedule
	}

	err = r.repository.SetCronIdAndChangeSchedule(r.jobsToAdd)
	if err != nil {
		t.Errorf("Fail to set cron id and change schedule: %s", err.Error())
	}

	// retrieve and check
	jobs, err = r.repository.GetJobsByIds(getIdsFromJobsWithScheduleList(r.jobsToAdd))
	for _, job := range jobs {
		if job.rawJob.CronID != newCronID {
			t.Errorf("Cron id not set. Got: %d Expected: %d\n", job.rawJob.CronID, newCronID)
		}
		if job.rawJob.CronExpression != newSchedule {
			t.Errorf("Schedule not changed. Got: %s Expected: %s\n", job.rawJob.CronExpression, newSchedule)
		}
	}
}

func (r *RepositoryTestSuite) TestList(t *testing.T) {
	// first, let's add all jobs
	err := r.repository.AddJobs(r.jobsToAdd)
	if err != nil {
		t.Errorf("Cannot add jobs: %s\n", err.Error())
		t.FailNow()
	}

	// and retrieve them
	jobs, err := r.repository.ListJobs(nil)
	if err != nil {
		t.Errorf("Cannot list jobs: %s", err.Error())
	}
	if len(jobs) != len(r.jobsToAdd) {
		t.Errorf("Count mismatch. Got: %d Expected: %d\n", len(jobs), len(r.jobsToAdd))
	}

	// now, pause one of them.
	err = r.repository.PauseJobs([]RawJob{r.jobsToAdd[0].rawJob})
	if err != nil {
		t.Errorf("Cannot pause job: %s\n", err.Error())
		t.FailNow()
	}

	// re-retrieve using paused = true
	paused := true
	options := ListJobsOptions{
		Paused:        &paused,
		GroupIDs:      nil,
		SuperGroupIDs: nil,
	}
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Cannot get paused jobs: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != 1 {
		t.Errorf("Paused = true count mismatch: Got %d Expected: %d\n", len(jobs), 1)
		t.FailNow()
	}

	// now do the same using paused = false
	paused = false
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Cannot get unpaused jobs: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(r.jobsToAdd)-1 {
		t.Errorf("Paused = false count mismatch: Got %d Expected: %d\n",
			len(jobs), len(r.jobsToAdd)-1)
		t.FailNow()
	}

	// resume the job that have been paused
	err = r.repository.ResumeJobs([]JobWithSchedule{r.jobsToAdd[0]})
	if err != nil {
		t.Errorf("Cannot resume job: %s\n", err.Error())
		t.FailNow()
	}

	// now, we grab the list of unique groups
	// and super groups
	groups := getUniqueGroupID(r.jobsToAdd)
	superGroups := getUniqueSuperGroupID(r.jobsToAdd)

	// query using all the available group ids
	// should return all jobs
	options.Paused = nil
	options.GroupIDs = groups
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by group ids: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(r.jobsToAdd) {
		t.Errorf("GroupIDs = all count mismatch. Got: %d Expected: %d\n",
			len(jobs), len(r.jobsToAdd))
		t.FailNow()
	}

	// now, let's pause a job within the group
	// to use the paused field
	err = r.repository.PauseJobs([]RawJob{{
		ID: jobs[0].ID,
	}})
	if err != nil {
		t.Errorf("Fail to pause jobs: %s\n", err.Error())
		t.FailNow()
	}
	paused = true
	options.Paused = &paused
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by group and paused: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != 1 {
		t.Errorf(`GroupIDs = all & paused = true count mismatch
Got: %d Expected: %d\n`, len(jobs), 1)
		t.FailNow()
	}

	// now test with pause = false
	paused = false
	options.Paused = &paused
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by group and paused: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(r.jobsToAdd)-1 {
		t.Errorf(`GroupIDs = all & paused = false count mismatch
Got: %d Expected: %d\n`, len(jobs), len(r.jobsToAdd)-1)
		t.FailNow()
	}

	// do the same, also for super groups
	options.Paused = nil
	options.GroupIDs = nil
	options.SuperGroupIDs = superGroups
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by super group ids: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(r.jobsToAdd) {
		t.Errorf("GroupIDs = all count mismatch. Got: %d Expected: %d\n",
			len(jobs), len(r.jobsToAdd))
		t.FailNow()
	}

	// now, select just a group
	group := groups[0]
	// compute all the jobs it should return
	var jobsByGroup []int64
	for _, job := range r.jobsToAdd {
		if job.rawJob.GroupID == group {
			jobsByGroup = append(jobsByGroup, job.rawJob.ID)
		}
	}
	options.GroupIDs = []int64{group}
	options.SuperGroupIDs = nil
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by subset of group ids: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(jobsByGroup) {
		t.Errorf("GroupIDs = groups[0] count mismatch. Got: %d Expected: %d\n",
			len(jobs), len(jobsByGroup))
		t.FailNow()
	}

	// do the same, also for the super-group
	// this time, using an array of more than one item.
	superGroupsToFilter := superGroups[len(superGroups)-2:]
	options.SuperGroupIDs = superGroupsToFilter
	options.GroupIDs = nil
	var jobsBySuperGroup []int64
	for _, job := range r.jobsToAdd {
		for _, superGroupId := range superGroupsToFilter {
			if job.rawJob.SuperGroupID == superGroupId {
				jobsBySuperGroup = append(jobsBySuperGroup, job.rawJob.ID)
				break
			}
		}
	}
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by subset of super group ids: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(jobsBySuperGroup) {
		t.Errorf("SuperGroupIDs = superGroups[-2:] count mismatch. Got: %d Expected: %d\n",
			len(jobs), len(jobsBySuperGroup))
		t.FailNow()
	}

	// now, we pause of such jobs
	err = r.repository.PauseJobs([]RawJob{{
		ID: jobs[0].ID,
	}})
	if err != nil {
		t.Errorf("Cannot pause jobs: %s\n", err.Error())
		t.FailNow()
	}
	paused = true
	options.Paused = &paused
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by paused and super group id: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != 1 {
		t.Errorf("SuperGroupIDs = superGroups[-2:] & paused = true count mismatch. Got: %d Expected: %d\n`",
			len(jobs), 1)
		t.FailNow()
	}

	// now, we do the same by with paused = false
	paused = false
	options.Paused = &paused
	jobs, err = r.repository.ListJobs(&options)
	if err != nil {
		t.Errorf("Fail to get jobs by paused and super group id: %s\n", err.Error())
		t.FailNow()
	}
	if len(jobs) != len(jobsBySuperGroup)-1 {
		t.Errorf(`SuperGroupIDs = superGroups[-2:] & paused = false count mismatch
Got: %d Expected: %d\n`, len(jobs), len(jobsBySuperGroup)-1)
		t.FailNow()
	}
}

// checkErrorIsOf checks that `err` is of type `expected`. If `err`
// is nil, fails showing `msg`.
func checkErrorIsOf(err, expected error, msg string, t *testing.T) {
	if err == nil {
		t.Errorf("%s error expected. Should have been not nil.\n", msg)
		t.FailNow()
	} else if !errors.Is(err, expected) {
		t.Errorf("Error is of wrong type: %s\n", err.Error())
		t.FailNow()
	}
}

// checkErrorMsg checks that `err` contains `msg`, failing otherwise.
// if `err` is nil it fails.
func checkErrorMsg(err error, msg string, t *testing.T) {
	if err == nil {
		t.Errorf("%s error expected. Should have been not nil.\n", msg)
		t.FailNow()
	}
	if !strings.Contains(err.Error(), msg) {
		t.Errorf("Error '%s' does not contain '%s'\n", err.Error(), msg)
		t.FailNow()
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
	_, err = r.repository.GetJobsByIds([]int64{10000})
	checkErrorIsOf(err, gorm.ErrRecordNotFound, "GetJobs", t)

	// get with many -- raw
	_, err = r.repository.ListJobs(&ListJobsOptions{JobIDs: []int64{10000}})
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
	// 3 jobs in (group 1, super group 1)
	// 1 job in (group 2, super group 1)
	// 1 job in (group 1, super group 2)
	// 1 job in (group 3, super group 3)
	r.jobsToAdd = []JobWithSchedule{
		{
			rawJob: RawJob{
				ID:             1,
				GroupID:        1,
				SuperGroupID:   1,
				CronExpression: "@every 60s",
			},
			run: &TestCronJobNoop{},
			runInput: CronJobInput{
				JobID:          1,
				GroupID:        1,
				SuperGroupID:   1,
				CronExpression: "@every 60s",
				OtherInputs: map[string]interface{}{
					"hello": "is there anybody in there?",
				},
			},
			schedule: scheduleNeverFail(t, 30),
		},
		{
			rawJob: RawJob{
				ID:             2,
				GroupID:        1,
				SuperGroupID:   1,
				CronExpression: "@every 120s",
			},
			run: &TestCronJobNoop{},
			runInput: CronJobInput{
				JobID:          2,
				GroupID:        1,
				SuperGroupID:   1,
				CronExpression: "@every 120s",
				OtherInputs: map[string]interface{}{
					"when I was a child": "I had fever",
				},
			},
			schedule: scheduleNeverFail(t, 60),
		},
		{
			rawJob: RawJob{
				ID:             3,
				GroupID:        1,
				SuperGroupID:   1,
				CronExpression: "@every 120s",
			},
			run: &TestCronJobModifyMap{},
			runInput: CronJobInput{
				JobID:          3,
				GroupID:        1,
				SuperGroupID:   1,
				CronExpression: "@every 120s",
				OtherInputs: map[string]interface{}{
					"when I was a child": "I had fever",
				},
			},
			schedule: scheduleNeverFail(t, 60),
		},
		{
			rawJob: RawJob{
				ID:             4,
				GroupID:        2,
				SuperGroupID:   1,
				CronExpression: "@every 120s",
			},
			run: &TestCronJobNoop{},
			runInput: CronJobInput{
				JobID:          4,
				GroupID:        2,
				SuperGroupID:   1,
				CronExpression: "@every 120s",
				OtherInputs: map[string]interface{}{
					"when I was a child": "I had fever",
				},
			},
			schedule: scheduleNeverFail(t, 60),
		},
		{
			rawJob: RawJob{
				ID:             5,
				GroupID:        1,
				SuperGroupID:   2,
				CronExpression: "@every 120s",
			},
			run: &TestCronJobNoop{},
			runInput: CronJobInput{
				JobID:          5,
				GroupID:        1,
				SuperGroupID:   2,
				CronExpression: "@every 120s",
				OtherInputs: map[string]interface{}{
					"when I was a child": "I had fever",
				},
			},
			schedule: scheduleNeverFail(t, 60),
		},
		{
			rawJob: RawJob{
				ID:             6,
				GroupID:        3,
				SuperGroupID:   3,
				CronExpression: "@every 120s",
			},
			run: &TestCronJobNoop{},
			runInput: CronJobInput{
				JobID:          6,
				GroupID:        3,
				SuperGroupID:   3,
				CronExpression: "@every 120s",
				OtherInputs: map[string]interface{}{
					"when I was a child": "I had fever",
				},
			},
			schedule: scheduleNeverFail(t, 60),
		},
	}
}

func (r *RepositoryTestSuite) teardown(okNotFound bool, t *testing.T) {
	err := r.repository.DeleteJobsByIds(getIdsFromJobsWithScheduleList(r.jobsToAdd))
	if err != nil {
		if !(okNotFound && errors.Is(err, r.repository.ErrorTypeIfMismatchCount())) {
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

func TestRepositoryList(t *testing.T) {
	tests := buildRepositoryTestSuite(t)

	for _, test := range tests {
		test.setup(t)
		test.TestList(t)
		test.teardown(true, t)
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

func getUniqueGroupID(jobs []JobWithSchedule) []int64 {
	var ids []int64
	for _, job := range jobs {
		found := false
		for _, id := range ids {
			if id == job.rawJob.GroupID {
				found = true
				break
			}
		}
		if found == false {
			ids = append(ids, job.rawJob.GroupID)
		}
	}
	return ids
}

func getUniqueSuperGroupID(jobs []JobWithSchedule) []int64 {
	var ids []int64
	for _, job := range jobs {
		found := false
		for _, id := range ids {
			if id == job.rawJob.SuperGroupID {
				found = true
				break
			}
		}
		if found == false {
			ids = append(ids, job.rawJob.SuperGroupID)
		}
	}
	return ids
}
