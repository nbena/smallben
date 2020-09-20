package smallben

import (
	"errors"
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

//import (
//	"fmt"
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	"testing"
//)
//
//func TestRepository(t *testing.T) {
//	RegisterFailHandler(Fail)
//	RunSpecs(t, "Repository")
//}
//
//var _ = Describe("Repository", func() {
//	var repository Repository
//	var availableUserEvaluationRules []UserEvaluationRule
//
//	BeforeEach(func() {
//		var err error
//
//		repositoryOptions := NewRepositoryOptions()
//		repository, err = NewRepository(&repositoryOptions)
//		Expect(err).ShouldNot(HaveOccurred())
//
//		availableUserEvaluationRules = []UserEvaluationRule{
//			{
//				Id:     1,
//				UserId: 1,
//				Tests: []Test{
//					{
//						Id:                   2,
//						EverySecond:          60,
//						UserId:               1,
//						UserEvaluationRuleId: 1,
//						Paused:               false,
//					}, {
//						Id:                   3,
//						EverySecond:          120,
//						UserId:               1,
//						UserEvaluationRuleId: 1,
//						Paused:               false,
//					},
//				},
//			},
//		}
//	})
//
//	// now test the adding of UserEvaluationRule
//	Context("when adding user evaluation rules", func() {
//
//		It("adds correctly those evaluation rules", func() {
//
//			// add them...
//			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//			// now performs a select making sure the adding is ok
//			result, err := repository.GetAllUserEvaluationRulesToExecute()
//			Expect(err).ShouldNot(HaveOccurred())
//			Expect(len(result)).To(Equal(len(availableUserEvaluationRules)))
//
//		})
//
//		Context("retrieves it", func() {
//			BeforeEach(func() {
//				err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
//				Expect(err).ShouldNot(HaveOccurred())
//			})
//
//			It("without errors", func() {
//				_, err := repository.GetUserEvaluationRule(availableUserEvaluationRules[0].Id)
//				Expect(err).ShouldNot(HaveOccurred())
//			})
//		})
//
//		AfterEach(func() {
//			// and we delete them to be sure we leave room for
//			// other operations
//			err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
//			Expect(err).ShouldNot(HaveOccurred())
//		})
//
//	})
//
//	Context("when deleting user evaluation rules", func() {
//
//		BeforeEach(func() {
//			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//		})
//
//		It("deletes  correctly those evaluation rules", func() {
//
//			// perform a delete
//			err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
//			Expect(err).ShouldNot(HaveOccurred())
//
//			// and then make sure those rules are not in the result
//			all, err := repository.GetAllUserEvaluationRulesToExecute()
//			Expect(err).ShouldNot(HaveOccurred())
//			allIds := getIdsFromUserEvaluationRuleList(all)
//			Expect(allIds).ToNot(ContainElements(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules)))
//		})
//	})
//
//	Context("pausing user evaluation rules", func() {
//
//		BeforeEach(func() {
//
//			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//		})
//
//		It("correctly pauses those user evaluation rules", func() {
//
//			// we set them to paused
//			// without need to modify the models
//			err := repository.PauseUserEvaluationRules(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//			// now we retrieve them
//			rules, err := repository.GetAllUserEvaluationRulesToExecute()
//			Expect(err).ShouldNot(HaveOccurred())
//
//			Expect(getIdsFromUserEvaluationRuleList(rules)).ToNot(ContainElements(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules)))
//		})
//
//		It("correctly resume those user evaluation rules", func() {
//
//			// re-pause them
//			err := repository.PauseUserEvaluationRules(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//			err = repository.ResumeUserEvaluationRule(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//			// now we retrieve them
//			rules, err := repository.GetAllUserEvaluationRulesToExecute()
//			Expect(err).ShouldNot(HaveOccurred())
//
//			Expect(len(availableUserEvaluationRules)).To(Equal(len(rules)))
//		})
//
//		AfterEach(func() {
//			// and we delete them to be sure we leave room for
//			// other operations
//			err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
//			Expect(err).ShouldNot(HaveOccurred())
//		})
//	})
//
//	Context("when setting the cron id of tests", func() {
//
//		var testsBefore []Test
//
//		BeforeEach(func() {
//			// add the UserEvaluationRule
//			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//			counter := 10
//			for _, rule := range availableUserEvaluationRules {
//				for i := range rule.Tests {
//					rule.Tests[i].CronId = counter
//					counter += 1
//					testsBefore = append(testsBefore, rule.Tests[i])
//				}
//			}
//		})
//
//		It("works", func() {
//
//			err := repository.SetCronIdOf(availableUserEvaluationRules)
//			Expect(err).ShouldNot(HaveOccurred())
//
//			rules, err := repository.GetAllUserEvaluationRulesToExecute()
//			Expect(err).ShouldNot(HaveOccurred())
//
//			testsAfter := flatTests(rules)
//
//			flags := make([]bool, len(testsAfter))
//
//			for i, testBefore := range testsBefore {
//				for _, testsAfter := range testsAfter {
//					if testBefore.Id == testsAfter.Id {
//						Expect(testBefore.CronId).To(Equal(testsAfter.CronId))
//						flags[i] = true
//						break
//					}
//				}
//			}
//
//			for i, flag := range flags {
//				Expect(flag).To(Equal(true), fmt.Sprintf("Not flagged: %v\n", testsBefore[i]))
//			}
//		})
//
//		AfterEach(func() {
//			// and we delete them to be sure we leave room for
//			// other operations
//			err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
//			Expect(err).ShouldNot(HaveOccurred())
//		})
//	})
//
//})
