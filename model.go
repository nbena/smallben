package smallben

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"time"
)

// To be used when dealing with generics.
type TestInfo interface {
	Id() int
	UserId() int
	CronId() int
	EverySecond() int
	Paused() bool
	CreatedAt() *time.Time
	UpdatedAt() *time.Time
	UserEvaluationRuleId() int
}

type Job struct {
	Id                   int32
	UserId               int32
	CronId               int32
	EverySecond          int32
	Paused               bool
	CreatedAt            time.Time
	UpdatedAt            time.Time
	UserEvaluationRuleId int32
}

func (t *Job) addToRaw() []interface{} {
	return []interface{}{
		t.Id,
		t.UserEvaluationRuleId,
		t.UserId,
		t.CronId,
		t.Paused,
		t.EverySecond,
		time.Now(),
		time.Now(),
	}
}

func addToColumn() []string {
	return []string{
		"id",
		"user_evaluation_rule_id",
		"user_id",
		"cron_id",
		"paused",
		"every_second",
		"created_at",
		"updated_at",
	}
}

type JobWithSchedule struct {
	Job
	Schedule cron.Schedule
}

// ToJobWithSchedule returns a JobWithSchedule object from the current Job,
// by copy. It returns errors in case the given schedule is not valid.
func (t *Job) ToJobWithSchedule() (JobWithSchedule, error) {
	var result JobWithSchedule
	schedule, err := cron.ParseStandard(fmt.Sprintf("@every %ds", t.EverySecond))
	if err != nil {
		return result, err
	}
	result = JobWithSchedule{
		Job: Job{
			Id:                   t.UserId,
			UserId:               t.UserId,
			CronId:               t.CronId,
			EverySecond:          t.EverySecond,
			Paused:               t.Paused,
			CreatedAt:            t.CreatedAt,
			UpdatedAt:            t.UpdatedAt,
			UserEvaluationRuleId: t.UserEvaluationRuleId,
		},
		Schedule: schedule,
	}
	return result, nil
}

func (t *Job) toRunFunctionInput() *runFunctionInput {
	return &runFunctionInput{
		testID:               t.Id,
		userEvaluationRuleId: t.UserEvaluationRuleId,
		userID:               t.UserId,
	}
}

func (t *Job) schedule() (cron.Schedule, error) {
	return cron.ParseStandard(fmt.Sprintf("@every {%d}s", t.EverySecond))
}

// GetIdsFromTestList basically does tests.map(test -> test.id)
func GetIdsFromTestList(tests []Job) []int32 {
	ids := make([]int32, len(tests))
	for i, test := range tests {
		ids[i] = test.Id
	}
	return ids
}

// UpdateSchedule is the struct used to update
// the schedule of a test.
type UpdateSchedule struct {
	// TestId is the ID of the tests
	TestId int32
	// EverySecond is the new schedule
	EverySecond int32
}

func (u *UpdateSchedule) schedule() (cron.Schedule, error) {
	return cron.ParseStandard(fmt.Sprintf("@every {%d}s", u.EverySecond))
}

// GetIdsFromUpdateScheduleList basically does schedules.map(test -> test.id)
func GetIdsFromUpdateScheduleList(schedules []UpdateSchedule) []int32 {
	ids := make([]int32, len(schedules))
	for i, test := range schedules {
		ids[i] = test.TestId
	}
	return ids
}
