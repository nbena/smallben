package smallben

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type SchedulerTestSuite struct {
	suite.Suite
	scheduler                    Scheduler
	availableUserEvaluationRules []UserEvaluationRule
}

func (s *SchedulerTestSuite) SetupTest() {
	s.scheduler = NewScheduler()
	s.scheduler.cron.Start()

	s.availableUserEvaluationRules = []UserEvaluationRule{
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
}

func (s *SchedulerTestSuite) TearDownTest() {
	ctx := s.scheduler.cron.Stop()
	<-ctx.Done()
}

func (s *SchedulerTestSuite) TestAdd() {
	modifiedRules, err := s.scheduler.AddUserEvaluationRule(s.availableUserEvaluationRules)
	s.Nil(err, "Error should not happen")

	// making sure all UserEvaluationRules have its own cron id
	for _, rule := range modifiedRules {
		for _, test := range rule.Tests {
			s.NotEqual(test.CronId, -1)
		}
	}

	// and they have all been added
	entries := s.scheduler.cron.Entries()
	s.Equal(len(entries), len(flatTests(modifiedRules)))
}

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SchedulerTestSuite))
}
