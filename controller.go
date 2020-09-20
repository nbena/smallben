package smallben

import "gorm.io/gorm"

type SmallBen struct {
	repository Repository
	scheduler  Scheduler
}

// Creates a new instance of SmallBen.
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

// Start starts the SmallBen, by starting the inner scheduler and filling it
// in with the needed UserEvaluationRule(s).
func (s *SmallBen) Start() error {
	s.scheduler.cron.Start()
	// now, we fill in the scheduler
	return s.Fill()
}

// Stop stops the SmallBen. This call will block until all *running* jobs
// have finished.
func (s *SmallBen) Stop() {
	ctx := s.scheduler.cron.Stop()
	// Wait on ctx.Done() till all jobs have finished, then left.
	<-ctx.Done()
}

// Retrieve all the UserEvaluationRule to execute from the database
// and then fills them into the scheduler.
// In case of errors, it is guaranteed that the scheduler and the database
// state won't change.
func (s *SmallBen) Fill() error {
	// get all the rules
	rules, err := s.repository.GetAllUserEvaluationRulesToExecute()
	if err != nil {
		return err
	}
	// now, add them to the scheduler
	modifiedRules, err := s.scheduler.AddUserEvaluationRule(rules)
	if err != nil {
		return err
	}
	// now, update the db by updating the cron entries
	err = s.repository.SetCronIdOf(modifiedRules)
	if err != nil {
		// if there is an error, remove them from the scheduler
		s.scheduler.DeleteUserEvaluationRules(modifiedRules)
	}
	return nil
}

// AddUserEvaluationRules add `rules`. It adds them to the scheduler and saves them to the db. If one
// of those operations fail, it is guaranteed that the state does not change.
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

// DeleteUserEvaluationRules deletes the UserEvaluationRule whose ids are in rulesID.
// This function returns an error if some of the requested rulesID are within the database.
func (s *SmallBen) DeleteUserEvaluationRules(rulesID []int) error {

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

//  PauseUserEvaluationRules pauses the UserEvaluationRule whose ids are in `rulesID`.
// The array must be not in excess, otherwise errors will be returned.
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

// ResumeUserEvaluationRules resumes the UserEvaluationRule whose id are in rulesID.
// The array must be not in excess, otherwise errors will be returned.
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
	err = s.repository.ResumeUserEvaluationRule(updatedRules)
	if err != nil {
		// if there errors, then re-remove from the scheduler in order
		// to keep the state in sync
		s.scheduler.DeleteUserEvaluationRules(rules)
		return err
	}
	return nil
}

// UpdateSchedule updates the inner state according to `schedules`.
// It is guaranteed that in case of any error, the database state won't be changed,
// and that the tests in `schedules` are *removed*.
func (s *SmallBen) UpdateSchedule(schedules []UpdateSchedule) error {

	// first, retrieve the tests from the database
	tests, err := s.repository.GetTests(GetIdsFromUpdateScheduleList(schedules))
	if err != nil {
		return err
	}

	// now, we build a list of tests from the schedules
	for _, schedule := range schedules {
		for _, test := range tests {
			if test.Id == schedule.TestId {
				test.EverySecond = schedule.EverySecond
				break
			}
		}
	}

	// delete the tests from the scheduler in case of errors.
	defer func() {
		if err != nil {
			s.scheduler.DeleteTests(tests)
		}
	}()

	// now, we can update such tests in the database
	// this, time, we need to open a transaction
	// here.
	err = s.repository.db.Transaction(func(tx *gorm.DB) error {

		// first, remove the objects from the scheduler.
		s.scheduler.DeleteTests(tests)

		// next, we perform the update in the database.
		for _, test := range tests {
			err := tx.Model(&test).Updates(map[string]interface{}{"every_second": test.EverySecond}).Error
			if err != nil {
				return err
			}
		}

		// now insert them in the scheduler
		modifiedTests, err := s.scheduler.AddTests(tests)
		if err != nil {
			return err
		}

		// now, update the cron id of such tests within the transaction
		for _, test := range modifiedTests {
			err = tx.Debug().Model(&test).Updates(map[string]interface{}{"cron_id": test.CronId, "paused": false}).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
