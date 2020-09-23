package smallben

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"testing"
)

var ctx = context.Background()

type RepositoryAddTestSuite struct {
	suite.Suite
	repository Repository2
	tests      []Job
}

func (r *RepositoryAddTestSuite) SetupTest() {
	repository, tests := setup(r.Suite)
	r.repository = repository
	r.tests = tests
}

func (r *RepositoryAddTestSuite) TestAdd() {
	schedules := buildSchedule(r)

	err := r.repository.AddTests(ctx, schedules)
	r.Nil(err, "Cannot add tests")

	// now performs a select making sure the adding is ok
	result, err := r.repository.GetAllTestsToExecute(ctx)
	r.Nil(err, "Cannot get rules")
	r.Equal(len(result), len(r.tests), "Len mismatch")
}

func (r *RepositoryAddTestSuite) TearDownTest() {
	teardown2(r, false)
}

type RepositoryOtherTestSuite struct {
	suite.Suite
	repository    Repository2
	tests         []Job
	okDeleteError bool
}

func (r *RepositoryOtherTestSuite) SetupTest() {
	repository, tests := setup(r.Suite)
	r.repository = repository
	r.tests = tests
	r.okDeleteError = false

	schedules := buildSchedule(r)

	// also add them
	err := r.repository.AddTests(ctx, schedules)
	r.Nil(err, "Cannot add tests on setup")
}

func (r *RepositoryOtherTestSuite) TearDownTest() {
	teardown2(r, true)
}

func (r *RepositoryOtherTestSuite) TestRetrieveSingle() {
	_, err := r.repository.GetTest(ctx, r.tests[0].Id)
	r.Nil(err, "Cannot retrieve single test")
}

func (r *RepositoryOtherTestSuite) TestDelete() {
	err := r.repository.DeleteTestsByKeys(ctx, GetIdsFromTestList(r.tests))
	r.Nil(err, "Cannot delete tests")
	r.okDeleteError = true
}

func (r *RepositoryOtherTestSuite) TestPause() {
	err := r.repository.PauseTests(ctx, r.tests)
	r.Nil(err, "Cannot pause tests")

	// now we retrieve them
	tests, err := r.repository.GetAllTestsToExecute(ctx)
	r.Nil(err, "Cannot retrieve tests")
	r.NotContains(GetIdsFromTestList(r.tests), GetIdsFromTestList(tests), "Contains failed")
}

func (r *RepositoryOtherTestSuite) TestResume() {
	err := r.repository.PauseTests(ctx, r.tests)
	r.Nil(err, "Cannot pause tests")

	err = r.repository.ResumeTests(ctx, r.tests)
	r.Nil(err, "Cannot resume tests")

	tests, err := r.repository.GetAllTestsToExecute(ctx)
	r.Nil(err, "Cannot retrieve tests")

	r.Equal(len(r.tests), len(tests), "Len mismatch")
}

func (r *RepositoryOtherTestSuite) TestChangeSchedule() {
	// grab a test to update
	test := r.tests[0]
	test.EverySecond += 50

	// create the array of tests
	tests := []JobWithSchedule{{
		Job: test,
	}}
	err := r.repository.SetCronIdAndChangeSchedule(ctx, tests)
	r.Nil(err, "Cannot change schedule")

	newTest, err := r.repository.GetTest(ctx, test.Id)
	r.Nil(err, "Cannot retrieve rule")

	r.Equal(newTest.EverySecond, test.EverySecond, "Update failed")
}

func (r *RepositoryOtherTestSuite) TestSetCronId() {
	var counter int32 = 10
	testsBefore := make([]Job, 0)
	for _, test := range r.tests {
		test.CronId = counter
		counter += 1
		testsBefore = append(testsBefore, test)
	}

	schedules := make([]JobWithSchedule, len(testsBefore))
	for i, test := range testsBefore {
		schedules[i] = JobWithSchedule{
			Job: test,
		}
	}

	err := r.repository.SetCronIdOfTestsWithSchedule(ctx, schedules)
	r.Nil(err, "Cannot set cron id of")

	testsAfter, err := r.repository.GetAllTestsToExecute(ctx)
	r.Nil(err, "Cannot retrieve tests")
	flags := make([]bool, len(testsAfter))

	for i, testBefore := range testsBefore {
		for _, testAfter := range testsAfter {
			if testBefore.Id == testAfter.Id {
				r.Equal(testBefore.CronId, testAfter.CronId, "CronId failed")
				flags[i] = true
				break
			}
		}
	}
	for i, flag := range flags {
		r.Equal(flag, true, fmt.Sprintf("Not flagged: %v", testsBefore[i]))
	}
}

func TestRepositoryTestSuite(t *testing.T) {
	suite.Run(t, new(RepositoryAddTestSuite))
}

func TestRepositoryOtherTestSuite(t *testing.T) {
	suite.Run(t, new(RepositoryOtherTestSuite))
}

// Interface used to encapsulate the behavior of the two tests struct.
type RepositoryTest interface {
	Tests() []Job
	Repository() *Repository2
	TestSuite() *suite.Suite
}

func (r *RepositoryAddTestSuite) Tests() []Job {
	return r.tests
}

func (r *RepositoryOtherTestSuite) Tests() []Job {
	return r.tests
}

func (r *RepositoryAddTestSuite) Repository() *Repository2 {
	return &r.repository
}

func (r *RepositoryOtherTestSuite) Repository() *Repository2 {
	return &r.repository
}

func (r *RepositoryAddTestSuite) TestSuite() *suite.Suite {
	return &r.Suite
}

func (r *RepositoryOtherTestSuite) TestSuite() *suite.Suite {
	return &r.Suite
}

func teardown2(t RepositoryTest, okError bool) {
	err := t.Repository().DeleteTestsByKeys(ctx, GetIdsFromTestList(t.Tests()))
	if err != nil {
		if !okError {
			fmt.Printf("To delete: %d test\n", len(t.Tests()))
			t.TestSuite().Nil(err, "Cannot delete tests")
		}
	}
}

func setup(suite suite.Suite) (Repository2, []Job) {
	ctx := context.Background()
	repositoryOptions, err := PgRepositoryOptionsFromEnv()
	suite.Nil(err, "Cannot get the correct config")
	if err != nil {
		suite.FailNow("Cannot go on.")
	}
	repository, err := NewRepository2(ctx, repositoryOptions)
	suite.Nil(err, "Cannot connect to the database")
	if err != nil {
		suite.FailNow("Cannot go on.")
	}
	tests := []Job{
		{
			Id:           2,
			EverySecond:  60,
			UserId:       1,
			SuperGroupId: 1,
			Paused:       false,
		}, {
			Id:           3,
			EverySecond:  120,
			UserId:       1,
			SuperGroupId: 1,
			Paused:       false,
		},
	}
	return repository, tests
}

func buildSchedule(r RepositoryTest) []JobWithSchedule {
	withSchedule := make([]JobWithSchedule, len(r.Tests()))
	for i, test := range r.Tests() {
		testWithSchedule, err := test.ToJobWithSchedule()
		r.TestSuite().Nil(err, "Cannot build test with schedule")
		if err != nil {
			r.TestSuite().FailNow("Cannot go on with test conversion fails")
		}
		withSchedule[i] = testWithSchedule
	}
	return withSchedule
}
