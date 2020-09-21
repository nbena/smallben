package smallben

import (
	"context"
	"gorm.io/gorm"
)

type SmallBen struct {
	repository Repository2
	scheduler  Scheduler
}

// Creates a new instance of SmallBen.
// The context is used to connect with the repository.
func (s *SmallBen) NewSmallBen(ctx context.Context, dbURL string) (SmallBen, error) {
	pgOptions, err := PgRepositoryOptions(dbURL)
	if err != nil {
		return SmallBen{}, err
	}
	database, err := NewRepository2(ctx, pgOptions)
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
// in with the needed Test.
func (s *SmallBen) Start(ctx context.Context) error {
	s.scheduler.cron.Start()
	// now, we fill in the scheduler
	return s.Fill(ctx)
}

// Stop stops the SmallBen. This call will block until all *running* jobs
// have finished.
func (s *SmallBen) Stop() {
	ctx := s.scheduler.cron.Stop()
	// Wait on ctx.Done() till all jobs have finished, then left.
	<-ctx.Done()
}

// Retrieve all the Test to execute from the database using ctx.
func (s *SmallBen) Fill(ctx context.Context) error {
	// get all the tests
	tests, err := s.repository.GetAllTestsToExecute(ctx)
	if err != nil {
		return err
	}
	return s.AddTests(ctx, tests)
}

// AddTests add `tests` to the scheduler, by saving also them to the database.
func (s *SmallBen) AddTests(ctx context.Context, tests []Test) error {
	// get all the tests
	tests, err := s.repository.GetAllTestsToExecute(ctx)
	if err != nil {
		return err
	}
	// now, build the TestWithSchedule object
	testsWithSchedule := make([]TestWithSchedule, len(tests))
	for i, test := range tests {
		testsWithSchedule[i], err = test.ToTestWithSchedule()
		if err != nil {
			return err
		}
	}
	// now, add them to the scheduler
	s.scheduler.AddTests2(testsWithSchedule)
	// now, update the db by updating the cron entries
	err = s.repository.SetCronIdOfTestsWithSchedule(ctx, testsWithSchedule)
	if err != nil {
		// if there is an error, remove them from the scheduler
		s.scheduler.DeleteTestsWithSchedule(testsWithSchedule)
	}
	return nil
}

// DeleteTests deletes `testsID` from the scheduler. It returns an error
// of type `pgx.ErrNoRows` if some of the tests have not been found.
func (s *SmallBen) DeleteTests(ctx context.Context, testsID []int32) error {
	// grab the tests
	// we need to know the cron id
	tests, err := s.repository.GetTestsByKeys(ctx, testsID)
	if err != nil {
		return err
	}

	// now delete them
	if err = s.repository.DeleteTestsByKeys(ctx, testsID); err != nil {
		return err
	}

	s.scheduler.DeleteTests(tests)
	return nil
}

// PauseTests pause the tests whose id are in `testsID`. It returns an error
// of type `pgx.ErrNoRows` if some of the tests have not been found.
func (s *SmallBen) PauseTests(ctx context.Context, testsID []int32) error {
	// grab the tests
	// we need to know the cron id
	tests, err := s.repository.GetTestsByKeys(ctx, testsID)
	if err != nil {
		return err
	}

	// now update them
	if err = s.repository.PauseTests(ctx, tests); err != nil {
		return err
	}
	s.scheduler.DeleteTests(tests)
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
			if test.Id == int32(schedule.TestId) {
				test.EverySecond = int32(schedule.EverySecond)
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
