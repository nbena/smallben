package smallben

import (
	"context"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
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
	// create the batch of requests
	var batch pgx.Batch
	var result pgx.BatchResults
	var err error

	defer func() {
		if err := result.Close(); err != nil {
			// log the error
		}
	}()

	for _, test := range tests {
		batch.Queue(`insert into tests
(id, user_evaluation_rule_id, user_id, paused, every_second, created_at, updated_at)
values
($1, $2, $3, false, $4, $5, $6)
`, test.Id, test.UserEvaluationRuleId, test.UserId, test.EverySecond, time.Now(), time.Now())
	}

	// now, send the batch requests
	// it is implicitly executed within a transaction.
	result = r.pool.SendBatch(ctx, &batch)
	_, err = result.Exec()
	return err
}

// GetTest returns a TestWithSchedule whose id is `testID`.
func (r *Repository2) GetTest(ctx context.Context, testID int) (TestWithSchedule, error) {
	var test Test
	err := pgxscan.Select(ctx, &r.pool, &test, `select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where id=$1`, testID)
	if err != nil {
		return TestWithSchedule{}, err
	}
	testWithSchedule, err := (&test).ToTestWithSchedule()
	return testWithSchedule, err
}

// PauseTests pauses `tests`. In case the number of affected rows is
//// different than the length of `tests`, `pgx.ErrNoRows` is returned,
func (r *Repository2) PauseTests(ctx context.Context, tests []Test) error {
	ids := GetIdsFromTestList(tests)
	result, err := r.pool.Exec(ctx, `update tests set paused = true where id in ($1)`, ids)
	if err != nil {
		return err
	}
	if result.RowsAffected() != int64(len(tests)) {
		return pgx.ErrNoRows
	}
	return err
}

// ResumeTests resumes `tests`. In case the number of affected rows is
// different than the length of `tests`, `pgx.ErrNoRows` is returned,
func (r *Repository2) ResumeTests(ctx context.Context, tests []Test) error {
	ids := GetIdsFromTestList(tests)
	result, err := r.pool.Exec(ctx, `update tests set paused = false where id in ($1)`, ids)
	if err != nil {
		return err
	}
	if result.RowsAffected() != int64(len(tests)) {
		return pgx.ErrNoRows
	}
	return nil
}

// GetAllTestsToExecute returns all the test whose `paused` field is set to `false`.
func (r *Repository2) GetAllTestsToExecute(ctx context.Context) ([]Test, error) {
	var tests []Test
	err := pgxscan.Select(ctx, &r.pool, &tests, `select select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where paused = false`)
	return tests, err
}

// GetTestsByKeys returns all the tests whose primary key are in `testsID`.
// It returns an error of kind `pgx.ErrNoRows` in case some tests
// are not found.
func (r *Repository2) GetTestsByKeys(ctx context.Context, testsID []int) ([]Test, error) {
	var tests []Test
	err := pgxscan.Select(ctx, &r.pool, &tests, `select select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where id in ($1)`, testsID)
	if err != nil {
		return nil, err
	}
	if len(tests) != len(testsID) {
		return nil, pgx.ErrNoRows
	}
	return tests, err
}

// DeleteTestsByKeys deletes the tests whose id are in `testsID`. It returns an error of
// type `pgx.ErrNoRows` in case the test has not been found.
func (r *Repository2) DeleteTestsByKeys(ctx context.Context, testsID []int) error {
	result, err := r.pool.Exec(ctx, "delete from tests where id in ($1)", testsID)
	if err != nil {
		return err
	}
	if result.RowsAffected() != int64(len(testsID)) {
		return pgx.ErrNoRows
	}
	return nil
}

// ChangeSchedule update `tests` by saving in the database the new schedule.
// Execution is done within a transaction. In case the number of affected rows
// is different than the length of `tests`, an error of kind `pgx.ErrNoRows` is returned.
func (r *Repository2) ChangeSchedule(ctx context.Context, tests []Test) error {
	// create the batch of requests
	var batch pgx.Batch
	var result pgx.BatchResults
	var err error

	defer func() {
		if err := result.Close(); err != nil {
			// log the error
		}
	}()

	// start the transaction
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	// prepare the batch request
	for _, test := range tests {
		batch.Queue("update tests set every_second = $2 where id = $1", test.Id, test.EverySecond)
	}

	// now, send the batch requests
	result = tx.SendBatch(ctx, &batch)
	rows, err := result.Exec()
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			// log the rollback error, there is nothing more we can do...
		}
		return err
	}
	if rows.RowsAffected() != int64(len(tests)) {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			// log the rollback error, there is nothing more we can do...
		}
		return pgx.ErrNoRows
	}
	return err
}

func (r *Repository2) SetCronId(tests []Test) error {
	return nil
}
