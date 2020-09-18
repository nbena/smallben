package smallben

type SmallBen struct {
	repository Repository
	scheduler  Scheduler
}

func (s *SmallBen) AddUserEvaluationRules(rules []UserEvaluationRule) error {

	// add them to the scheduler
	// since we need the id assigned by the scheduler
	modifiedRules, err := s.scheduler.AddUserEvaluationRule(rules)
	if err != nil {
		return err
	}
	// and now to the database
	err = s.repository.AddUserEvaluationRule(modifiedRules)
	if err != nil {
		// still, we might have some errors returned from
		// the database. In that case, we clean up the scheduler
		// once again
		s.scheduler.DeleteUserEvaluationRules(modifiedRules)
	}
	return nil
}

func (s *SmallBen) DeleteUserEvaluationRule(rulesID []int) error {
	// first, we grab the rule from the database
	rules := make([]UserEvaluationRule, len(rulesID))

	for i, ruleID := range rulesID {
		uer, err := s.repository.GetUserEvaluationRule(ruleID)
		if err != nil {
			return err
		}
		rules[i] = uer
	}

	s.scheduler.DeleteUserEvaluationRules(rules)
	return nil
}

//func (s *SmallBen) AddUserEvaluationRule(userEvaluationRule *UserEvaluationRule) error {
//	// first, we need to add the job to the scheduler, in order
//	// to get back the id
//
//	inputs := userEvaluationRule.toRunFunctionInput()
//
//	for i, runFunctionInput := range inputs {
//		entryId, err := s.cron.AddFunc(getCronSchedule(userEvaluationRule.Tests[i].Seconds), func() {
//			runFunction(runFunctionInput)
//		})
//		if err != nil {
//			return err
//		}
//		// set the entryId to the test object
//		// need to convert since it is an opaque type
//		userEvaluationRule.Tests[i].CronId = int(entryId)
//	}
//
//	// now, we can add the entries to the database.
//	return nil
//}
