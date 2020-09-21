package smallben

import (
	"context"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"time"
)

// Repository2 is used to manage the operations within the Postgres
// backend. It is created with the function `NewRepository2`.
type Repository2 struct {
	pool pgxpool.Pool
}

// NewRepository2 returns an instance of Repository2.
func NewRepository2(ctx context.Context, options *pgxpool.Config) (Repository2, error) {
	pool, err := pgxpool.ConnectConfig(ctx, options)
	if err != nil {
		return Repository2{}, err
	}
	return Repository2{
		pool: *pool,
	}, nil
}

// AddTests add `tests` within to the database. The update is done by using a batch
// in a transaction.
func (r *Repository2) AddTests(ctx context.Context, tests []Test) error {
	rows := make([][]interface{}, len(tests))
	for i, test := range tests {
		rows[i] = test.addToRaw()
	}

	copyCount, err := r.pool.CopyFrom(ctx, pgx.Identifier{"tests"}, addToColumn(), pgx.CopyFromRows(rows))
	if err != nil {
		return err
	}
	if copyCount != int64(len(tests)) {
		return pgx.ErrNoRows
	}
	return nil
}

// GetTest returns a TestWithSchedule whose id is `testID`.
func (r *Repository2) GetTest(ctx context.Context, testID int32) (TestWithSchedule, error) {
	var tests []Test
	err := pgxscan.Select(ctx, &r.pool, &tests, `select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where id=$1`, testID)
	if err != nil {
		return TestWithSchedule{}, err
	}
	if len(tests) == 0 {
		return TestWithSchedule{}, pgx.ErrNoRows
	}
	test := tests[0]
	testWithSchedule, err := (&test).ToTestWithSchedule()
	return testWithSchedule, err
}

// PauseTests pauses `tests`, i.e., changing the `paused` field to `true`.
func (r *Repository2) PauseTests(ctx context.Context, tests []Test) error {
	ids := GetIdsFromTestList(tests)
	_, err := r.pool.Exec(ctx, `update tests set paused = true where id = any($1)`, ids)
	if err != nil {
		return err
	}
	return err
}

// ResumeTests resumes `tests`, i.e., changing the `paused` field to `false`.
func (r *Repository2) ResumeTests(ctx context.Context, tests []Test) error {
	ids := GetIdsFromTestList(tests)
	_, err := r.pool.Exec(ctx, `update tests set paused = false where id = any($1)`, ids)
	if err != nil {
		return err
	}
	return nil
}

// GetAllTestsToExecute returns all the test whose `paused` field is set to `false`.
func (r *Repository2) GetAllTestsToExecute(ctx context.Context) ([]Test, error) {
	var tests []Test
	err := pgxscan.Select(ctx, &r.pool, &tests, `select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where paused = false`)
	return tests, err
}

// GetTestsByKeys returns all the tests whose primary key are in `testsID`. It returns an
// error of type `pgx.ErrNoRow` in case there is a mismatch between the length of the returned
// tests and of the input.
func (r *Repository2) GetTestsByKeys(ctx context.Context, testsID []int32) ([]Test, error) {
	var tests []Test
	err := pgxscan.Select(ctx, &r.pool, &tests, `select select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where id = any($1)`, testsID)
	if err != nil {
		return nil, err
	}
	if len(tests) != len(testsID) {
		return nil, pgx.ErrNoRows
	}
	return tests, err
}

// DeleteTestsByKeys deletes the tests whose id are in `testsID`.
func (r *Repository2) DeleteTestsByKeys(ctx context.Context, testsID []int32) error {
	_, err := r.pool.Exec(ctx, "delete from tests where id = any($1)", testsID)
	if err != nil {
		return err
	}
	return nil
}

// ChangeSchedule update `tests` by saving in the database the new schedule.
// Execution is done within a transaction.
func (r *Repository2) ChangeSchedule(ctx context.Context, tests []Test) error {
	return r.transactionUpdate(
		ctx,
		tests,
		func(test *Test, batch *pgx.Batch) {
			batch.Queue("update tests set every_second = $2, updated_at = $3 where id = $1",
				test.Id, test.EverySecond, time.Now())
		})
}

// ChangeScheduleOfTestsWithSchedule update `tests` by saving in the database the new schedule.
// Execution is done within a transaction.
func (r *Repository2) ChangeScheduleOfTestsWithSchedule(ctx context.Context, tests []TestWithSchedule) error {
	rawTests := make([]Test, len(tests))
	for i, test := range tests {
		rawTests[i] = test.Test
	}
	return r.transactionUpdate(
		ctx,
		rawTests,
		func(test *Test, batch *pgx.Batch) {
			batch.Queue("update tests set every_second = $2, updated_at = $3 where id = $1",
				test.Id, test.EverySecond, time.Now())
		})
}

// SetCronId updates `tests` by updating the `cron_id` field.
func (r *Repository2) SetCronId(ctx context.Context, tests []Test) error {
	return r.transactionUpdate(
		ctx,
		tests,
		func(test *Test, batch *pgx.Batch) {
			batch.Queue("update tests set cron_id = $2, updated_at = $3 where id = $1",
				test.Id, test.CronId, time.Now())
		},
	)
}

// SetCronIdOfTestWithSchedule updates `tests` by updating the `cron_id` field.
func (r *Repository2) SetCronIdOfTestsWithSchedule(ctx context.Context, tests []TestWithSchedule) error {
	rawTests := make([]Test, len(tests))
	for i, test := range tests {
		rawTests[i] = test.Test
	}
	return r.transactionUpdate(
		ctx,
		rawTests,
		func(test *Test, batch *pgx.Batch) {
			batch.Queue("update tests set cron_id = $2, updated_at = $3 where id = $1",
				test.Id, test.CronId, time.Now())
		},
	)
}

// SetCronIdAndChangeSchedule updates the `cron_id` and the `every_second` field.
func (r *Repository2) SetCronIdAndChangeSchedule(ctx context.Context, tests []TestWithSchedule) error {
	rawTests := make([]Test, len(tests))
	for i, test := range tests {
		rawTests[i] = test.Test
	}
	return r.transactionUpdate(
		ctx,
		rawTests,
		func(test *Test, batch *pgx.Batch) {
			batch.Queue("update tests set cron_id = $2, every_second = $3 updated_at = $4 where id = $1",
				test.Id, test.CronId, test.EverySecond, time.Now())
		},
	)
}

func (r *Repository2) transactionUpdate(ctx context.Context, tests []Test, getBatchFn func(test *Test, batch *pgx.Batch)) error {
	// create the batch of requests
	var batch pgx.Batch
	var result pgx.BatchResults
	var err error

	// start the transaction
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	log.Printf("Begin transaction\n")

	// prepare the batch request
	for _, test := range tests {
		getBatchFn(&test, &batch)
	}

	// now, send the batch requests
	result = tx.SendBatch(ctx, &batch)
	_, err = result.Exec()
	if err != nil {
		log.Printf("Got error on result.Exec()")
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			log.Printf("Got error on tx.Rollback(), err: %s", err.Error())
			// log the rollback error, there is nothing more we can do...
		}
		return err
	}
	// before commit, I need to close the result
	if err = result.Close(); err != nil {
		// try rolling back
		log.Printf("Error on closing the result: %s\n", err.Error())
		if err = tx.Rollback(ctx); err != nil {
			log.Printf("Error on rollback: %s\n", err.Error())
			// nothing more to do to.
			return err
		}
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		log.Printf("Error on commit: %s\n", err.Error())
		// well, what do? Just try to rollback
		if err = tx.Rollback(ctx); err != nil {
			log.Printf("Error on rollback: %s\n", err.Error())
			// nothing more to do to.
			return err
		}
		return err
	}
	return nil
}
