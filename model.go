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

type Test struct {
	Id                   int32
	UserId               int32
	CronId               int32
	EverySecond          int32
	Paused               bool
	CreatedAt            time.Time
	UpdatedAt            time.Time
	UserEvaluationRuleId int32
}

func (t *Test) addToRaw() []interface{} {
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

type TestWithSchedule struct {
	Test
	Schedule cron.Schedule
}

// ToTestWithSchedule returns a TestWithSchedule object from the current Test,
// by copy. It returns errors in case the given schedule is not valid.
func (t *Test) ToTestWithSchedule() (TestWithSchedule, error) {
	var result TestWithSchedule
	schedule, err := cron.ParseStandard(fmt.Sprintf("@every %ds", t.EverySecond))
	if err != nil {
		return result, err
	}
	result = TestWithSchedule{
		Test: Test{
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

func (t *Test) toRunFunctionInput() *runFunctionInput {
	return &runFunctionInput{
		testID:               t.Id,
		userEvaluationRuleId: t.UserEvaluationRuleId,
		userID:               t.UserId,
	}
}

func (t *Test) schedule() (cron.Schedule, error) {
	return cron.ParseStandard(fmt.Sprintf("@every {%d}s", t.EverySecond))
}

type UserEvaluationRule struct {
	Id        int `gorm:"primaryKey"`
	UserId    int
	CreatedAt time.Time
	UpdatedAt time.Time
	Tests     []Test
}

func (u *UserEvaluationRule) toRunFunctionInput() []runFunctionInput {
	inputs := make([]runFunctionInput, len(u.Tests))
	for i, test := range u.Tests {
		inputs[i] = runFunctionInput{
			testID:               test.Id,
			userID:               int32(u.UserId),
			userEvaluationRuleId: int32(u.Id),
		}
	}
	return inputs
}

// FlatTests returns a flattened list of Test contained in `rules`, i.e.,
// rules.map(rule -> rule.tests).flatten().
func FlatTests(rules []UserEvaluationRule) []Test {
	var tests []Test
	for _, rule := range rules {
		tests = append(tests, rule.Tests...)
	}
	return tests
}

// GetIdsFromUserEvaluationRuleList basically does rules.map(rule -> rule.id)
func GetIdsFromUserEvaluationRuleList(rules []UserEvaluationRule) []int {
	ids := make([]int, len(rules))
	for i, rule := range rules {
		ids[i] = rule.Id
	}
	return ids
}

// GetIdsFromTestList basically does tests.map(test -> test.id)
func GetIdsFromTestList(tests []Test) []int32 {
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
