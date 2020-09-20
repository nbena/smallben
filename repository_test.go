package smallben

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"testing"
)

type RepositoryAddTestSuite struct {
	suite.Suite
	repository                   Repository
	availableUserEvaluationRules []UserEvaluationRule
}

func (r *RepositoryAddTestSuite) SetupTest() {
	repository, rules := setup(r.Suite)
	r.repository = repository
	r.availableUserEvaluationRules = rules
}

func (r *RepositoryAddTestSuite) TestAdd() {
	err := r.repository.AddUserEvaluationRule(r.availableUserEvaluationRules)
	r.Nil(err, "Cannot add rules")

	// now performs a select making sure the adding is ok
	result, err := r.repository.GetAllUserEvaluationRulesToExecute()
	r.Nil(err, "Cannot get rules")
	r.Equal(len(result), len(r.availableUserEvaluationRules), "Len mismatch")
}

func (r *RepositoryAddTestSuite) TearDownTest() {
	teardown(r.Suite, &r.repository, r.availableUserEvaluationRules)
}

type RepositoryOtherTestSuite struct {
	suite.Suite
	repository                   Repository
	availableUserEvaluationRules []UserEvaluationRule
	okDeleteError                bool
}

func (r *RepositoryOtherTestSuite) SetupTest() {
	repository, rules := setup(r.Suite)
	r.repository = repository
	r.availableUserEvaluationRules = rules
	r.okDeleteError = false
	// also add them
	err := r.repository.AddUserEvaluationRule(r.availableUserEvaluationRules)
	r.Nil(err, "Cannot add rules on setup")
}

func (r *RepositoryOtherTestSuite) TearDownTest() {
	err := r.repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(r.availableUserEvaluationRules))
	if r.okDeleteError {
		r.Suite.True(errors.Is(err, gorm.ErrRecordNotFound))
	} else {
		r.Nil(err)
	}
}

func (r *RepositoryOtherTestSuite) TestRetrieveSingle() {
	_, err := r.repository.GetUserEvaluationRule(r.availableUserEvaluationRules[0].Id)
	r.Nil(err, "Cannot retrieve single rule")
}

func (r *RepositoryOtherTestSuite) TestDelete() {
	err := r.repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(r.availableUserEvaluationRules))
	r.Nil(err, "Cannot delete rules")
	r.okDeleteError = true
}

func (r *RepositoryOtherTestSuite) TestPause() {
	err := r.repository.PauseUserEvaluationRules(r.availableUserEvaluationRules)
	r.Nil(err, "Cannot pause rules")

	// now we retrieve them
	rules, err := r.repository.GetAllUserEvaluationRulesToExecute()
	r.Nil(err, "Cannot retrieve rules")
	r.NotContains(getIdsFromUserEvaluationRuleList(rules),
		getIdsFromUserEvaluationRuleList(r.availableUserEvaluationRules), "Contains failed")
}

func (r *RepositoryOtherTestSuite) TestResume() {
	err := r.repository.PauseUserEvaluationRules(r.availableUserEvaluationRules)
	r.Nil(err, "Cannot pause rules")

	err = r.repository.ResumeUserEvaluationRule(r.availableUserEvaluationRules)
	r.Nil(err, "Cannot resume rules")

	rules, err := r.repository.GetAllUserEvaluationRulesToExecute()
	r.Nil(err, "Cannot retrieve rules")

	r.Equal(len(r.availableUserEvaluationRules), len(rules), "Len mismatch")
}

func (r *RepositoryOtherTestSuite) TestSetCronId() {
	counter := 10
	testsBefore := make([]Test, 0)
	for _, rule := range r.availableUserEvaluationRules {
		for i := range rule.Tests {
			rule.Tests[i].CronId = counter
			counter += 1
			testsBefore = append(testsBefore, rule.Tests[i])
		}
	}

	err := r.repository.SetCronIdOf(r.availableUserEvaluationRules)
	r.Nil(err, "Cannot set cron id of")

	rules, err := r.repository.GetAllUserEvaluationRulesToExecute()
	r.Nil(err, "Cannot retrieve rules")

	testsAfter := flatTests(rules)
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

func teardown(suite suite.Suite, repository *Repository, rules []UserEvaluationRule) {
	err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(rules))
	suite.Nil(err, "Cannot delete rules")
}

func setup(suite suite.Suite) (Repository, []UserEvaluationRule) {
	repositoryOptions := NewRepositoryOptions()
	repository, err := NewRepository(&repositoryOptions)
	suite.Nil(err, "Cannot connect to the database")
	availableUserEvaluationRules := []UserEvaluationRule{
		{
			Id:     1,
			UserId: 1,
			Tests: []Test{
				{
					Id:                   2,
					EverySecond:          60,
					UserId:               1,
					UserEvaluationRuleId: 1,
					Paused:               false,
				}, {
					Id:                   3,
					EverySecond:          120,
					UserId:               1,
					UserEvaluationRuleId: 1,
					Paused:               false,
				},
			},
		},
	}
	return repository, availableUserEvaluationRules
}
