package smallben

import (
	"bytes"
	"encoding/gob"
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
	// ID is a unique ID identifying the job object.
	// It is chosen by the user.
	ID int64
	// GroupID is the ID of the group this job is inserted in.
	GroupID int64
	// SuperGroupID specifies the ID of the super group
	// where this group is contained in.
	SuperGroupID int64
	// CronID is the ID of the cron job as assigned by the scheduler
	// internally.
	CronID int64
	// EverySecond specifies every how many seconds the job will run.
	EverySecond int64
	// Paused specifies whether this job has been paused.
	Paused bool
	// CreatedAt specifies when this job has been created.
	CreatedAt time.Time
	// UpdatedAt specifies the last time this object has been updated,
	// i.e., paused/resumed/schedule updated.
	UpdatedAt time.Time
	// serializedJob is the gob-encoded byte array
	// of the interface executing this job
	serializedJob []byte
	// serializedJobInput is the gob-encoded byte array
	// of the input of the interface executing this job
	serializedJobInput []byte
}

func (t *Job) addToRaw() []interface{} {
	return []interface{}{
		t.ID,
		t.GroupID,
		t.SuperGroupID,
		t.CronID,
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
	job      Job
	schedule cron.Schedule
	run      CronJob
	runInput CronJobInput
}

// Schedule returns the schedule used by this object.
func (j *JobWithSchedule) Schedule() *cron.Schedule {
	return &j.schedule
}

// ToJobWithSchedule returns a JobWithSchedule object from the current Job,
// by copy. It returns errors in case the given schedule is not valid,
// or in case the conversion of the job interface/input fails.
func (t *Job) ToJobWithSchedule() (JobWithSchedule, error) {
	var result JobWithSchedule
	// decode the schedule
	schedule, err := cron.ParseStandard(fmt.Sprintf("@every %ds", t.EverySecond))
	if err != nil {
		return result, err
	}
	var buffer *bytes.Buffer
	var decoder *gob.Decoder
	// decode the interface
	buffer = bytes.NewBuffer(t.serializedJob)
	decoder = gob.NewDecoder(buffer)
	var runJob CronJob
	if err = decoder.Decode(&runJob); err != nil {
		return result, err
	}
	// decode the input of the job function
	buffer = bytes.NewBuffer(t.serializedJobInput)
	decoder = gob.NewDecoder(buffer)
	var runJobInput CronJobInput
	if err = decoder.Decode(&runJobInput); err != nil {
		return result, err
	}
	result = JobWithSchedule{
		job: Job{
			ID:           t.ID,
			GroupID:      t.GroupID,
			SuperGroupID: t.SuperGroupID,
			CronID:       t.CronID,
			EverySecond:  t.EverySecond,
			Paused:       t.Paused,
			CreatedAt:    t.CreatedAt,
			UpdatedAt:    t.UpdatedAt,
		},
		schedule: schedule,
		run:      runJob,
		runInput: runJobInput,
	}
	return result, nil
}

func (t *Job) schedule() (cron.Schedule, error) {
	return cron.ParseStandard(fmt.Sprintf("@every {%d}s", t.EverySecond))
}

func (j *JobWithSchedule) BuildJob() (Job, error) {
	var encoder *gob.Encoder
	encoder = gob.NewEncoder(bytes.NewBuffer(j.job.serializedJob))
	if err := encoder.Encode(j.run); err != nil {
		return Job{}, err
	}
	encoder = gob.NewEncoder(bytes.NewBuffer(j.job.serializedJobInput))
	if err := encoder.Encode(j.runInput); err != nil {
		return Job{}, err
	}
	return j.job, nil
}

// GetIdsFromJobsList basically does jobs.map(job -> job.id)
func GetIdsFromJobsList(jobs []Job) []int64 {
	ids := make([]int64, len(jobs))
	for i, test := range jobs {
		ids[i] = test.ID
	}
	return ids
}

// GetIdsFromJobsWithScheduleList basically does jobs.map(job -> job.id)
func GetIdsFromJobsWithScheduleList(jobs []JobWithSchedule) []int64 {
	ids := make([]int64, len(jobs))
	for i, job := range jobs {
		ids[i] = job.job.ID
	}
	return ids
}

// UpdateSchedule is the struct used to update
// the schedule of a test.
type UpdateSchedule struct {
	// JobID is the ID of the tests
	JobID int64
	// EverySecond is the new schedule
	EverySecond int64
}

func (u *UpdateSchedule) schedule() (cron.Schedule, error) {
	return cron.ParseStandard(fmt.Sprintf("@every {%d}s", u.EverySecond))
}

// GetIdsFromUpdateScheduleList basically does schedules.map(job -> job.id)
func GetIdsFromUpdateScheduleList(schedules []UpdateSchedule) []int64 {
	ids := make([]int64, len(schedules))
	for i, test := range schedules {
		ids[i] = test.JobID
	}
	return ids
}

// CronJobInput is the input passed to the Run function.
type CronJobInput struct {
	JobID        int64
	GroupID      int64
	SuperGroupID int64
	OtherInputs  map[string]interface{}
}

// CronJob is the interface jobs has to implement.
// It contains only one single method, `Run`.
type CronJob interface {
	Run(input CronJobInput)
}
