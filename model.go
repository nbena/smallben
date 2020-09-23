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

// Job is the struct used to interact with the
// persistent scheduler.
type Job struct {
	// Id is a unique ID identifying the job object.
	// It is chosen by the user.
	Id int32
	// GroupId is the ID of the group this job is inserted in.
	GroupId int32
	// CronId is the ID of the cron job as assigned by the scheduler
	// internally.
	CronId int32
	// EverySecond specifies every how many seconds the job will run.
	EverySecond int32
	// Paused specifies whether this job has been paused.
	Paused bool
	// CreatedAt specifies when this job has been created.
	CreatedAt time.Time
	// UpdatedAt specifies the last time this object has been updated,
	// i.e., paused/resumed/schedule updated.
	UpdatedAt time.Time
	// SuperGroupId specifies the ID of the super group
	// where this group is contained in.
	SuperGroupId int32
}

func (t *Job) addToRaw() []interface{} {
	return []interface{}{
		t.Id,
		t.GroupId,
		t.SuperGroupId,
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
		"group_id",
		"super_group_id",
		"cron_id",
		"paused",
		"every_second",
		"created_at",
		"updated_at",
	}
}

// JobWithSchedule is a job object
// with a cron.Schedule object in it.
// The schedule can be accessed by using the Schedule()
// method.
// This object should be created only by calling the method
// ToJobWithSchedule().
type JobWithSchedule struct {
	Job
	schedule cron.Schedule
}

// Schedule returns the schedule used by this object.
func (j *JobWithSchedule) Schedule() *cron.Schedule {
	return &j.schedule
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
			Id:           t.Id,
			GroupId:      t.GroupId,
			SuperGroupId: t.SuperGroupId,
			CronId:       t.CronId,
			EverySecond:  t.EverySecond,
			Paused:       t.Paused,
			CreatedAt:    t.CreatedAt,
			UpdatedAt:    t.UpdatedAt,
		},
		schedule: schedule,
	}
	return result, nil
}

func (t *Job) toRunFunctionInput() *runFunctionInput {
	return &runFunctionInput{
		jobID:        t.Id,
		groupID:      t.GroupId,
		superGroupID: t.SuperGroupId,
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
