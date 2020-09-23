package smallben

//type SchedulerTestSuite struct {
//	suite.Suite
//	scheduler                    Scheduler
//	availableUserEvaluationRules []UserEvaluationRule
//}
//
//func (s *SchedulerTestSuite) SetupTest() {
//	s.scheduler = NewScheduler()
//	s.scheduler.cron.Start()
//
//	s.availableUserEvaluationRules = []UserEvaluationRule{
//		{
//			ID:     1,
//			UserId: 1,
//			Tests: []Job{
//				{
//					ID:                   2,
//					EverySecond:          60,
//					UserId:               1,
//					SuperGroupID: 1,
//					Paused:               false,
//				}, {
//					ID:                   3,
//					EverySecond:          120,
//					UserId:               1,
//					SuperGroupID: 1,
//					Paused:               false,
//				},
//			},
//		},
//	}
//}
//
//func (s *SchedulerTestSuite) TearDownTest() {
//	s.scheduler.DeleteUserEvaluationRules(s.availableUserEvaluationRules)
//	ctx := s.scheduler.cron.Stop()
//	<-ctx.Done()
//}
//
//func (s *SchedulerTestSuite) TestAdd() {
//	modifiedRules, err := s.scheduler.AddUserEvaluationRule(s.availableUserEvaluationRules)
//	s.Nil(err, "Error should not happen")
//
//	// making sure all UserEvaluationRules have its own cron id
//	for _, rule := range modifiedRules {
//		for _, test := range rule.Tests {
//			s.NotEqual(test.CronID, -1)
//		}
//	}
//
//	// and they have all been added
//	entries := s.scheduler.cron.Entries()
//	s.Equal(len(entries), len(FlatTests(modifiedRules)))
//}
//
//func (s *SchedulerTestSuite) TestDelete() {
//	modifiedRules, err := s.scheduler.AddUserEvaluationRule(s.availableUserEvaluationRules)
//	s.Nil(err, "Error should not happen")
//	// length of the inserted rules
//	lenBefore := len(s.scheduler.cron.Entries())
//
//	s.scheduler.DeleteUserEvaluationRules(modifiedRules)
//	lenAfter := len(s.scheduler.cron.Entries())
//
//	s.Equal(lenAfter+len(FlatTests(s.availableUserEvaluationRules)), lenBefore, "len mismatch")
//}
//
//func TestSchedulerSuite(t *testing.T) {
//	suite.Run(t, new(SchedulerTestSuite))
//}
