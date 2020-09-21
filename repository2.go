package smallben

import (
	"context"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
)

type Repository2 struct {
	pool pgxpool.Pool
}

// AddTests add `tests` within to the database. The update is done by using a batch
// in a transaction.
func (r *Repository2) AddTests(tests []TestInfo) error {
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
`, test.Id(), test.UserEvaluationRuleId(), test.UserEvaluationRuleId(), test.EverySecond(), time.Now(), time.Now())
	}

	// now, send the batch requests
	// it is implicitly executed within a transaction.
	result = r.pool.SendBatch(context.Background(), &batch)
	_, err = result.Exec()
	return err
}

// GetTest returns a TestWithSchedule whose id is `testID`.
func (r *Repository2) GetTest(testID int) (TestWithSchedule, error) {
	var test Test
	err := pgxscan.Select(context.Background(), &r.pool, &test, `select id, user_id, cron_id, every_second,
paused, created_at, updated_at, user_evaluation_rule_id from tests where id=$1`, testID)
	if err != nil {
		return TestWithSchedule{}, err
	}
	testWithSchedule, err := (&test).ToTestWithSchedule()
	return testWithSchedule, err
}
