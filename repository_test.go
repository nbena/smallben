package smallben

import (
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

	BeforeEach(func() {
		var err error
		// var db *sql.DB
		repositoryOptions := NewRepositoryOptions()
		repository, err = NewRepository(&repositoryOptions)
		Expect(err).ShouldNot(HaveOccurred())
	})

	// now test the adding of UserEvaluationRule
	Context("add user evaluation rules", func() {

		var toSave []UserEvaluationRule

		BeforeEach(func() {
			// create the data we need
			toSave = []UserEvaluationRule{
				{
					Id:     1,
					UserId: 1,
					Tests: []Test{
						{
							Id:                   1,
							EverySecond:          60,
							UserId:               1,
							UserEvaluationRuleId: 1,
						}, {
							Id:                   2,
							EverySecond:          120,
							UserId:               1,
							UserEvaluationRuleId: 1,
						},
					},
				},
			}
		})

		It("add", func() {

			err := repository.AddUserEvaluationRule(toSave)
			Expect(err).ShouldNot(HaveOccurred())

			// now performs a select making sure the adding is ok
			result, err := repository.GetAllUserEvaluationRulesToExecute()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(result)).To(Equal(len(toSave)))

		})
	})

})
