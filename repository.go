package smallben

//import (
//	"context"
//	"github.com/georgysavva/scany/pgxscan"
//	"github.com/jackc/pgx/v4"
//	"github.com/jackc/pgx/v4/pgxpool"
//	"log"
//	"time"
//)
//
//// Repository2 is used to manage the operations within the Postgres
//// backend. It is created with the function `NewRepository2`.
//type Repository2 struct {
//	pool pgxpool.Pool
//}
//
//// NewRepository2 returns an instance of Repository2.
//func NewRepository2(ctx context.Context, options *pgxpool.Config) (Repository2, error) {
//	pool, err := pgxpool.ConnectConfig(ctx, options)
//	if err != nil {
//		return Repository2{}, err
//	}
//	return Repository2{
//		pool: *pool,
//	}, nil
//}
//
//// AddJobs add `jobsToAdd` within to the database. The update is done within a transaction.
//func (r *Repository2) AddJobs(ctx context.Context, jobsToAdd []JobWithSchedule) error {
//	rows := make([][]interface{}, len(jobsToAdd))
//	for i, test := range jobsToAdd {
//		rows[i] = test.addToRaw()
//	}
//
//	copyCount, err := r.pool.CopyFrom(ctx, pgx.Identifier{"jobsToAdd"}, addToColumn(), pgx.CopyFromRows(rows))
//	if err != nil {
//		return err
//	}
//	if copyCount != int64(len(jobsToAdd)) {
//		return pgx.ErrNoRows
//	}
//	return nil
//}
//
//// GetJob returns a JobWithSchedule whose id is `jobID`.
//// It returns an already converted JobWithSchedule.
//func (r *Repository2) GetJob(ctx context.Context, jobID int32) (JobWithSchedule, error) {
//	var jobsToAdd []RawJob
//	err := pgxscan.Select(ctx, &r.pool, &jobsToAdd, `select id, group_id, super_group_id, cron_id,
//every_second, paused, created_at, updated_at from jobsToAdd where id=$1`, jobID)
//	if err != nil {
//		return JobWithSchedule{}, err
//	}
//	if len(jobsToAdd) == 0 {
//		return JobWithSchedule{}, pgx.ErrNoRows
//	}
//	rawJob := jobsToAdd[0]
//	jobWithSchedule, err := (&rawJob).ToJobWithSchedule()
//	return jobWithSchedule, err
//}
//
//// PauseJobs pauses `jobsToAdd`, i.e., changing the `paused` field to `true`.
//func (r *Repository2) PauseJobs(ctx context.Context, jobsToAdd []RawJob) error {
//	ids := GetIdsFromJobRawList(jobsToAdd)
//	_, err := r.pool.Exec(ctx, `update jobsToAdd set paused = true where id = any($1)`, ids)
//	if err != nil {
//		return err
//	}
//	return err
//}
//
//// ResumeJobs resumes `jobsToAdd`, i.e., changing the `paused` field to `false`.
//func (r *Repository2) ResumeJobs(ctx context.Context, jobsToAdd []RawJob) error {
//	ids := GetIdsFromJobRawList(jobsToAdd)
//	_, err := r.pool.Exec(ctx, `update jobsToAdd set paused = false where id = any($1)`, ids)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//// GetAllJobsToExecute returns all the jobsToAdd whose `paused` field is set to `false`.
//func (r *Repository2) GetAllJobsToExecute(ctx context.Context) ([]RawJob, error) {
//	var jobsToAdd []RawJob
//	err := pgxscan.Select(ctx, &r.pool, &jobsToAdd, `select id, group_id, super_group_id, cron_id,
//every_second, paused, created_at, updated_at from jobsToAdd where paused = false`)
//	return jobsToAdd, err
//}
//
//func (r *Repository2) GetAllJobsToExecute2(ctx context.Context) ([]JobWithSchedule, error) {
//	var rawJobs []RawJob
//	err := pgxscan.Select(ctx, &r.pool, &rawJobs, `select id, group_id, super_group_id, cron_id,
//every_second, paused, created_at, updated_at from jobsToAdd where paused = false`)
//	if err != nil {
//		return nil, err
//	}
//	jobsToAdd := make([]JobWithSchedule, len(rawJobs))
//	for i, rawJob := range rawJobs {
//		rawJob, err := rawJob.ToJobWithSchedule()
//		if err != nil {
//			return nil, err
//		}
//		jobsToAdd[i] = rawJob
//	}
//	return jobsToAdd, nil
//}
//
//// GetJobsByIds returns all the jobsToAdd whose IDs are in `jobsID`. It returns an
//// error of type `pgx.ErrNoRow` in case there is a mismatch between the length of the returned
//// jobsToAdd and of the input.
//func (r *Repository2) GetJobsByIds(ctx context.Context, jobsID []int32) ([]RawJob, error) {
//	var jobsToAdd []RawJob
//	err := pgxscan.Select(ctx, &r.pool, &jobsToAdd, `select select id, group_id, super_group_id, cron_id,
//every_second, paused, created_at, updated_at from jobsToAdd where id = any($1)`, jobsID)
//	if err != nil {
//		return nil, err
//	}
//	if len(jobsToAdd) != len(jobsID) {
//		return nil, pgx.ErrNoRows
//	}
//	return jobsToAdd, err
//}
//
//// DeleteJobsByIds deletes the jobsToAdd whose id are in `jobsID`.
//func (r *Repository2) DeleteJobsByIds(ctx context.Context, jobsID []int32) error {
//	_, err := r.pool.Exec(ctx, "delete from jobsToAdd where id = any($1)", jobsID)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
////// ChangeSchedule update `tests` by saving in the database the new schedule.
////// Execution is done within a transaction.
////func (r *Repository2) ChangeSchedule(ctx context.Context, tests []RawJob) error {
////	return r.transactionUpdate(
////		ctx,
////		tests,
////		func(test *RawJob, batch *pgx.Batch) {
////			batch.Queue("update tests set every_second = $2, updated_at = $3 where id = $1",
////				test.ID, test.CronExpression, time.Now())
////		})
////}
//
////// ChangeScheduleOfTestsWithSchedule update `tests` by saving in the database the new schedule.
////// Execution is done within a transaction.
////func (r *Repository2) ChangeScheduleOfTestsWithSchedule(ctx context.Context, tests []JobWithSchedule) error {
////	rawTests := make([]RawJob, len(tests))
////	for i, test := range tests {
////		rawTests[i] = test.RawJob
////	}
////	return r.transactionUpdate(
////		ctx,
////		rawTests,
////		func(test *RawJob, batch *pgx.Batch) {
////			batch.Queue("update tests set every_second = $2, updated_at = $3 where id = $1",
////				test.ID, test.CronExpression, time.Now())
////		})
////}
//
//// SetCronIdOfJobsWithSchedule updates `jobsToAdd` by updating the `CronID` field (and the `UpdatedAt`).
//func (r *Repository2) SetCronIdOfJobsWithSchedule(ctx context.Context, jobsToAdd []JobWithSchedule) error {
//	rawTests := make([]RawJob, len(jobsToAdd))
//	for i, test := range jobsToAdd {
//		rawTests[i] = test.rawJob
//	}
//	return r.transactionUpdate(
//		ctx,
//		rawTests,
//		func(test *RawJob, batch *pgx.Batch) {
//			batch.Queue("update jobsToAdd set cron_id = $2, updated_at = $3 where id = $1",
//				test.ID, test.CronID, time.Now())
//		},
//	)
//}
//
//// SetCronIdAndChangeSchedule updates the `CronID` and the `CronExpression` field (and the `UpdatedAt`).
//func (r *Repository2) SetCronIdAndChangeSchedule(ctx context.Context, jobsToAdd []JobWithSchedule) error {
//	rawTests := make([]RawJob, len(jobsToAdd))
//	for i, test := range jobsToAdd {
//		rawTests[i] = test.rawJob
//	}
//	return r.transactionUpdate(
//		ctx,
//		rawTests,
//		func(test *RawJob, batch *pgx.Batch) {
//			batch.Queue("update jobsToAdd set cron_id = $2, every_second = $3 updated_at = $4 where id = $1",
//				test.ID, test.CronID, test.CronExpression, time.Now())
//		},
//	)
//}
//
//func (r *Repository2) transactionUpdate(ctx context.Context, tests []RawJob, getBatchFn func(test *RawJob, batch *pgx.Batch)) error {
//	// create the batch of requests
//	var batch pgx.Batch
//	var result pgx.BatchResults
//	var err error
//
//	// start the transaction
//	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
//	if err != nil {
//		return err
//	}
//
//	log.Printf("Begin transaction\n")
//
//	// prepare the batch request
//	for _, test := range tests {
//		getBatchFn(&test, &batch)
//	}
//
//	// now, send the batch requests
//	result = tx.SendBatch(ctx, &batch)
//	_, err = result.Exec()
//	if err != nil {
//		log.Printf("Got error on result.Exec()")
//		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
//			log.Printf("Got error on tx.Rollback(), err: %s", err.Error())
//			// log the rollback error, there is nothing more we can do...
//		}
//		return err
//	}
//	// before commit, I need to close the result
//	if err = result.Close(); err != nil {
//		// try rolling back
//		log.Printf("Error on closing the result: %s\n", err.Error())
//		if err = tx.Rollback(ctx); err != nil {
//			log.Printf("Error on rollback: %s\n", err.Error())
//			// nothing more to do to.
//			return err
//		}
//		return err
//	}
//	if err = tx.Commit(ctx); err != nil {
//		log.Printf("Error on commit: %s\n", err.Error())
//		// well, what do? Just try to rollback
//		if err = tx.Rollback(ctx); err != nil {
//			log.Printf("Error on rollback: %s\n", err.Error())
//			// nothing more to do to.
//			return err
//		}
//		return err
//	}
//	return nil
//}
