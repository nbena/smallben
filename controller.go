package smallben

import (
	"context"
)

// SmallBen is the struct managing the persistent
// scheduler state.
type SmallBen struct {
	repository Repository2
	scheduler  Scheduler
}

// NewSmallBen creates a new instance of SmallBen.
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
	return nil
}

func (s *SmallBen) ResumeTests(ctx context.Context, testsID []int32) error {
	// grab the tests
	// we need to know the cron id
	tests, err := s.repository.GetTestsByKeys(ctx, testsID)
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
	// resume them in the database
	if err = s.repository.ResumeTests(ctx, tests); err != nil {
		return err
	}
	// and now update them in the scheduler
	s.scheduler.DeleteTestsWithSchedule(testsWithSchedule)
	s.scheduler.AddTests2(testsWithSchedule)

	// now, update in the database by setting the cron id
	if err = s.repository.SetCronIdOfTestsWithSchedule(ctx, testsWithSchedule); err != nil {
		// remove from the scheduler also
		s.scheduler.DeleteTests(tests)
		return err
	}
	return nil
}

// UpdateSchedule updates the scheduler internal state by changing the `scheduleInfo`
// of the required tests.
// It is guaranteed that the state does not change in case of any error.
func (s *SmallBen) UpdateSchedule(ctx context.Context, scheduleInfo []UpdateSchedule) error {
	// first, we grab all the tests
	tests, err := s.repository.GetTestsByKeys(ctx, GetIdsFromUpdateScheduleList(scheduleInfo))
	if err != nil {
		return err
	}

	// the tests with the new required schedule
	testsWithScheduleNew := make([]TestWithSchedule, len(scheduleInfo))
	// the tests with the old schedule
	testsWithScheduleOld := make([]TestWithSchedule, len(scheduleInfo))

	// now, we compute the new schedule while also
	// keeping a copy of the old one
	for i, test := range tests {
		// compute the schedule of the old one
		testWithScheduleOld, err := test.ToTestWithSchedule()
		if err != nil {
			// should never happen, but...
			return err
		}
		// insert the test with the old schedule in the list
		testsWithScheduleOld[i] = testWithScheduleOld

		// make a copy of it
		testRawNew := testWithScheduleOld.Test
		// and update the everySecond parameter
		testRawNew.EverySecond = scheduleInfo[i].EverySecond
		// in order to compute the new schedule
		testWithScheduleNew, err := testRawNew.ToTestWithSchedule()
		if err != nil {
			return err
		}
		// and insert them in the list
		testsWithScheduleNew[i] = testWithScheduleNew
	}

	// now, we remove the tests from scheduler
	// it is safe to remove tests from the scheduler even if they
	// are not in the scheduler
	s.scheduler.DeleteTestsWithSchedule(testsWithScheduleNew)

	// now, we (re-)add them to the scheduler
	s.scheduler.AddTests2(testsWithScheduleNew)

	// now, we update the cron id in the database and also we update
	// the schedule. And we're done
	if err = s.repository.SetCronIdAndChangeSchedule(ctx, testsWithScheduleNew); err != nil {
		// in this case, we remove them from the scheduler
		s.scheduler.DeleteTestsWithSchedule(testsWithScheduleNew)
		// and we add the old ones
		s.scheduler.AddTests2(testsWithScheduleOld)
	}
	return nil
}
