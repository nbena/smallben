package smallben

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"time"
)

type Test struct {
	Id                   int `gorm:"primaryKey"`
	UserId               int
	CronId               int  `gorm:"default:0"`
	EverySecond          int  `gorm:"check:every_second >= 60"`
	Paused               bool `gorm:"default:false"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
	UserEvaluationRuleId uint `gorm:"column:user_evaluation_rule_id"`
}

func (t *Test) toRunFunctionInput() *runFunctionInput {
	return &runFunctionInput{
		testID:               t.Id,
		userEvaluationRuleId: int(t.UserEvaluationRuleId),
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
			userID:               u.UserId,
			userEvaluationRuleId: u.Id,
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
func GetIdsFromTestList(tests []Test) []int {
	ids := make([]int, len(tests))
	for i, test := range tests {
		ids[i] = test.Id
	}
	return ids
}

// UpdateSchedule is the struct used to update
// the schedule of a test.
type UpdateSchedule struct {
	// TestId is the ID of the tests
	TestId int
	// EverySecond is the new schedule
	EverySecond int
}

// GetIdsFromUpdateScheduleList basically does schedules.map(test -> test.id)
func GetIdsFromUpdateScheduleList(schedules []UpdateSchedule) []int {
	ids := make([]int, len(schedules))
	for i, test := range schedules {
		ids[i] = test.TestId
	}
	return ids
}
