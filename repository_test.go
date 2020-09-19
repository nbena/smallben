package smallben

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Repository")
}

var _ = Describe("Repository", func() {
	var repository Repository
	var availableUserEvaluationRules []UserEvaluationRule

	BeforeEach(func() {
		var err error

		repositoryOptions := NewRepositoryOptions()
		repository, err = NewRepository(&repositoryOptions)
		Expect(err).ShouldNot(HaveOccurred())

		availableUserEvaluationRules = []UserEvaluationRule{
			{
				Id:     1,
				UserId: 1,
				Tests: []Test{
					{
						Id:                   1,
						EverySecond:          60,
						UserId:               1,
						UserEvaluationRuleId: 1,
						Paused:               false,
					}, {
						Id:                   2,
						EverySecond:          120,
						UserId:               1,
						UserEvaluationRuleId: 1,
						Paused:               false,
					},
				},
			},
		}
	})

	// now test the adding of UserEvaluationRule
	Context("when adding user evaluation rules", func() {

		It("adds correctly those evaluation rules", func() {

			// add them...
			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
			Expect(err).ShouldNot(HaveOccurred())

			// now performs a select making sure the adding is ok
			result, err := repository.GetAllUserEvaluationRulesToExecute()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(result)).To(Equal(len(availableUserEvaluationRules)))

		})

		AfterEach(func() {
			// and we delete them to be sure we leave room for
			// other operations
			err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
			Expect(err).ShouldNot(HaveOccurred())
		})

	})

	Context("when deleting user evaluation rules", func() {

		BeforeEach(func() {
			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("deletes  correctly those evaluation rules", func() {

			// perform a delete
			err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
			Expect(err).ShouldNot(HaveOccurred())

			// and then make sure those rules are not in the result
			all, err := repository.GetAllUserEvaluationRulesToExecute()
			Expect(err).ShouldNot(HaveOccurred())
			allIds := getIdsFromUserEvaluationRuleList(all)
			Expect(allIds).ToNot(ContainElements(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules)))
		})
	})

	Context("pausing user evaluation rules", func() {

		BeforeEach(func() {

			err := repository.AddUserEvaluationRule(availableUserEvaluationRules)
			Expect(err).ShouldNot(HaveOccurred())

		})

		It("correctly pauses those user evaluation rules", func() {

			// we set them to paused
			// without need to modify the models
			fmt.Printf("=============\n")

			err := repository.PauseUserEvaluationRules(availableUserEvaluationRules)
			Expect(err).ShouldNot(HaveOccurred())

			// now we retrieve them

			rules, err := repository.GetAllUserEvaluationRulesToExecute()
			Expect(err).ShouldNot(HaveOccurred())

			fmt.Printf("%v\n", rules[0].Tests)

			Expect(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules)).ToNot(ContainElements(getIdsFromUserEvaluationRuleList(rules)))
		})

		//AfterEach(func() {
		//	// and we delete them to be sure we leave room for
		//	// other operations
		//	err := repository.DeleteUserEvaluationRules(getIdsFromUserEvaluationRuleList(availableUserEvaluationRules))
		//	Expect(err).ShouldNot(HaveOccurred())
		//})
	})

})
