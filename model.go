package smallben

import "time"

type Test struct {
	Id                   int `gorm:"primaryKey"`
	UserId               int
	CronId               int  `gorm:"default:0"`
	EverySecond          int  `gorm:"check:seconds >= 60"`
	Paused               bool `gorm:"default:false"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
	UserEvaluationRuleId uint
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
