package smallben

type SmallBen struct {
	repository Repository
	scheduler  Scheduler
}

func (s *SmallBen) NewSmallBen(dbOptions *RepositoryOptions) (SmallBen, error) {
	database, err := NewRepository(dbOptions)
	if err != nil {
		return SmallBen{}, nil
	}
	scheduler := NewScheduler()
	return SmallBen{
		repository: database,
		scheduler:  scheduler,
	}, nil
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

	rules, err := s.getUserEvaluationRulesFromIds(rulesID)
	if err != nil {
		return err
	}

	s.scheduler.DeleteUserEvaluationRules(rules)
	return nil
}

// Returns a list of UserEvaluationRule whose id is in `rulesID`. The list of UserEvaluationRule id
// must be complete and not in excess.
func (s *SmallBen) getUserEvaluationRulesFromIds(rulesID []int) ([]UserEvaluationRule, error) {
	rules := make([]UserEvaluationRule, len(rulesID))
	for i, ruleID := range rulesID {
		uer, err := s.repository.GetUserEvaluationRule(ruleID)
		if err != nil {
			return nil, err
		}
		rules[i] = uer
	}
	return rules, nil
}

// Pause the UserEvaluationRule whose id is in `rulesID`.
func (s *SmallBen) PauseUserEvaluationRules(rulesID []int) error {
	// first, grab the list of UserEvaluationRule to pause
	rules, err := s.getUserEvaluationRulesFromIds(rulesID)
	if err != nil {
		return err
	}

	// let's set them to pause in the database first
	if err := s.repository.PauseUserEvaluationRules(rules); err != nil {
		return err
	}

	// pause them from the scheduler means just to remove them
	s.scheduler.DeleteUserEvaluationRules(rules)
	return nil
}

func (s *SmallBen) ResumeUserEvaluationRules(rulesID []int) error {
	// first, grab the list of UserEvaluationRule to pause
	rules, err := s.getUserEvaluationRulesFromIds(rulesID)
	if err != nil {
		return err
	}

	// add them to the scheduler, in order to get back the id
	updatedRules, err := s.scheduler.AddUserEvaluationRule(rules)
	if err != nil {
		return err
	}

	// now, add them to the database
	err = s.repository.ResumeUserEvaluationRule(updatedRules, true)
	if err != nil {
		// if there errors, then re-remove from the scheduler in order
		// to keep the state in sync
		s.scheduler.DeleteUserEvaluationRules(rules)
		return err
	}
	return nil
}
