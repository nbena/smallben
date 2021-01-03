package smallben

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/robfig/cron/v3"
	"time"
)

// Job is the struct used to interact with SmallBen.
type Job struct {
	// ID is a unique ID identifying the rawJob object.
	// It is chosen by the user.
	ID int64
	// GroupID is the ID of the group this rawJob is inserted in.
	GroupID int64
	// SuperGroupID specifies the ID of the super group
	// where this group is contained in.
	SuperGroupID int64
	// cronID is the ID of the cron rawJob as assigned by the scheduler
	// internally.
	cronID int64
	// CronExpression specifies the scheduling of the job.
	CronExpression string
	// paused specifies whether this job has been paused.
	// Only used when returning this struct.
	paused bool
	// createdAt specifies when this rawJob has been created.
	createdAt time.Time
	// updatedAt specifies the last time this object has been updated,
	// i.e., paused/resumed/schedule updated.
	updatedAt time.Time
	// Job is the real unit of work to be executed
	Job CronJob
	// JobInput is the additional input to pass to the inner Job.
	JobInput map[string]interface{}
}

// CreatedAt returns the time when this Job has been added to the scheduler.
func (j *Job) CreatedAt() time.Time {
	return j.createdAt
}

// UpdatedAt returns the last time this Job has been updated, i.e.,
// paused, resumed, schedule changed.
func (j *Job) UpdatedAt() time.Time {
	return j.updatedAt
}

// Paused returns whether this Job is currently paused
// or not.
func (j *Job) Paused() bool {
	return j.paused
}

// toJobWithSchedule converts Job to a JobWithSchedule object.
// It returns an error in case the parsing of the cron expression fails.
func (j *Job) toJobWithSchedule() (JobWithSchedule, error) {
	var result JobWithSchedule
	// decode the schedule
	schedule, err := cron.ParseStandard(j.CronExpression)
	if err != nil {
		return result, err
	}

	result = JobWithSchedule{
		rawJob: RawJob{
			ID:             j.ID,
			GroupID:        j.GroupID,
			SuperGroupID:   j.SuperGroupID,
			CronID:         0,
			CronExpression: j.CronExpression,
			Paused:         false,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		},
		schedule: schedule,
		run:      j.Job,
		runInput: CronJobInput{
			JobID:          j.ID,
			GroupID:        j.GroupID,
			SuperGroupID:   j.SuperGroupID,
			OtherInputs:    j.JobInput,
			CronExpression: j.CronExpression,
		},
	}
	return result, nil
}

// RawJob models a raw rawJob coming from the database.
type RawJob struct {
	// ID is a unique ID identifying the rawJob object.
	// It is chosen by the user.
	ID int64 `gorm:"primaryKey,column:id"`
	// GroupID is the ID of the group this rawJob is inserted in.
	GroupID int64 `gorm:"column:group_id"`
	// SuperGroupID specifies the ID of the super group
	// where this group is contained in.
	SuperGroupID int64 `gorm:"column:super_group_id"`
	// CronID is the ID of the cron rawJob as assigned by the scheduler
	// internally.
	CronID int64 `gorm:"column:cron_id"`
	// CronExpression specifies the scheduling of the job.
	CronExpression string `gorm:"column:cron_expression"`
	// Paused specifies whether this rawJob has been paused.
	Paused bool `gorm:"column:paused"`
	// CreatedAt specifies when this rawJob has been created.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt specifies the last time this object has been updated,
	// i.e., paused/resumed/schedule updated.
	UpdatedAt time.Time `gorm:"column:updated_at"`
	// SerializedJob is the base64(gob-encoded byte array)
	// of the interface executing this rawJob
	SerializedJob string `gorm:"column:serialized_job"`
	// SerializedJobInput is the base64(gob-encoded byte array)
	// of the map containing the argument for the job.
	SerializedJobInput string `gorm:"column:serialized_job_input"`
}

func (j *RawJob) TableName() string {
	return "jobs"
}

// JobWithSchedule is a RawJob object
// with a cron.Schedule object in it.
// The schedule can be accessed by using the Schedule() method.
// This object should be created only by calling the method
// toJobWithSchedule().
type JobWithSchedule struct {
	rawJob   RawJob
	schedule cron.Schedule
	run      CronJob
	runInput CronJobInput
}

// decodeSerializedFields decode j.serializedJob and j.SerializedJobInput.
func (j *RawJob) decodeSerializedFields() (CronJob, CronJobInput, error) {
	var decoder *gob.Decoder
	var err error

	// decode from base64 the serialized job
	decodedJob, err := base64.StdEncoding.DecodeString(j.SerializedJob)
	if err != nil {
		return nil, CronJobInput{}, err
	}

	// decode the interface executing the rawJob
	decoder = gob.NewDecoder(bytes.NewBuffer(decodedJob))
	var runJob CronJob
	if err = decoder.Decode(&runJob); err != nil {
		return nil, CronJobInput{}, err
	}

	// decode the input from json
	var jobInputMap map[string]interface{}
	if err := json.Unmarshal([]byte(j.SerializedJobInput), &jobInputMap); err != nil {
		return nil, CronJobInput{}, err
	}

	// and build the overall object containing all the
	// inputs will be passed to the Job
	runJobInput := CronJobInput{
		JobID:          j.ID,
		GroupID:        j.GroupID,
		SuperGroupID:   j.SuperGroupID,
		CronExpression: j.CronExpression,
		OtherInputs:    jobInputMap,
	}
	return runJob, runJobInput, nil
}

// toJob converts j to a Job instance.
func (j *RawJob) toJob() (Job, error) {
	job, jobInput, err := j.decodeSerializedFields()
	if err != nil {
		return Job{}, err
	}
	result := Job{
		ID:             j.ID,
		GroupID:        j.GroupID,
		SuperGroupID:   j.SuperGroupID,
		cronID:         j.CronID,
		CronExpression: j.CronExpression,
		paused:         j.Paused,
		createdAt:      j.CreatedAt,
		updatedAt:      j.UpdatedAt,
		Job:            job,
		JobInput:       jobInput.OtherInputs,
	}
	return result, nil
}

// ToJobWithSchedule returns a JobWithSchedule object from the current RawJob,
// by copy. It returns errors in case the given schedule is not valid,
// or in case the conversion of the rawJob interface/input fails.
// It does NOT copy the byte arrays from j.
func (j *RawJob) ToJobWithSchedule() (JobWithSchedule, error) {
	var result JobWithSchedule
	// decode the schedule
	schedule, err := cron.ParseStandard(j.CronExpression)
	if err != nil {
		return result, err
	}
	runJob, runJobInput, err := j.decodeSerializedFields()
	if err != nil {
		return result, err
	}

	result = JobWithSchedule{
		rawJob: RawJob{
			ID:             j.ID,
			GroupID:        j.GroupID,
			SuperGroupID:   j.SuperGroupID,
			CronID:         j.CronID,
			CronExpression: j.CronExpression,
			Paused:         j.Paused,
			CreatedAt:      j.CreatedAt,
			UpdatedAt:      j.UpdatedAt,
		},
		schedule: schedule,
		run:      runJob,
		runInput: runJobInput,
	}
	return result, nil
}

// encodeJob encodes `job`. A separate function is needed because we need to pass
// a POINTER to interface.
func encodeJob(encoder *gob.Encoder, job CronJob) error {
	return encoder.Encode(&job)
}

// BuildJob builds the raw version of the inner job, by encoding
// it. In particular, the encoding is done as follows:
// - for the serialized job, it is encoded in Gob and then in base64
// - for the job input, it is encoded in json.
//This is needed since, when converting from a `RawJob` to a `JobWithSchedule`,
// the binary serialization of the Job is not kept in memory.
func (j *JobWithSchedule) BuildJob() (RawJob, error) {
	var bufferJob bytes.Buffer
	encoderJob := gob.NewEncoder(&bufferJob)
	// encode the CronJob interface keeping the unit of work
	// to execute. We need to use the encodeJob method
	// due to how gob interface encoding works.
	if err := encodeJob(encoderJob, j.run); err != nil {
		return RawJob{}, err
	}
	// finally, encode the bytes to base64
	j.rawJob.SerializedJob = base64.StdEncoding.EncodeToString(bufferJob.Bytes())

	// now, encode the job input
	if err := j.encodeJobInput(); err != nil {
		return RawJob{}, err
	}
	return j.rawJob, nil
}

// encodeJobInput encodes j.rawJob.SerializedJobInput.
func (j *JobWithSchedule) encodeJobInput() error {
	encodedInput, err := json.Marshal(j.runInput.OtherInputs)
	if err != nil {
		return err
	}
	j.rawJob.SerializedJobInput = string(encodedInput)
	return nil
}

// getIdsFromJobRawList basically does jobs.map(rawJob -> rawJob.id)
func getIdsFromJobRawList(jobs []RawJob) []int64 {
	ids := make([]int64, len(jobs))
	for i, test := range jobs {
		ids[i] = test.ID
	}
	return ids
}

// getIdsFromJobsWithScheduleList basically does jobs.map(rawJob -> rawJob.id)
func getIdsFromJobsWithScheduleList(jobs []JobWithSchedule) []int64 {
	ids := make([]int64, len(jobs))
	for i, job := range jobs {
		ids[i] = job.rawJob.ID
	}
	return ids
}

// getIdsFromJobList basically does jobs.map(rawJob -> rawJob.id)
func getIdsFromJobList(jobs []Job) []int64 {
	ids := make([]int64, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	return ids
}

// UpdateOption is the struct used to update
// a Job.
//
// An update on a Job consists in changing
// the schedule, by using the field `CronExpression`,
// or the Job input, by using the field `JobInput`.
//
// If none of those fields are specified, i.e., they are
// both nil, the struct is considered invalid.
type UpdateOption struct {
	// JobID is the ID of the Job to update.
	JobID int64
	// CronExpression is the new schedule of the Job.
	// If nil, it is ignored, i.e., the schedule
	// is not changed.
	CronExpression *string
	// JobInput is the new JobInput of the Job.
	// If nil, it is ignored, i.e.,
	// the Job input is not changed.
	JobInput *map[string]interface{}
}

func (u *UpdateOption) schedule() (cron.Schedule, error) {
	return cron.ParseStandard(*u.CronExpression)
}

var (
	// ErrUpdateOptionInvalid is returned when the fields
	// of UpdateOption are invalid.
	// This error is returned when the combination
	// of the fields is not valid (i.e., both nil).
	// For error in the CronExpression field,
	// the specific error set by the library is returned,
	ErrUpdateOptionInvalid = errors.New("invalid option")
)

// Valid returns whether the fields in this struct
// are valid. If the struct is valid, no errors
// are returned.
//
// UpdateOption is considered valid if
// at least one field between CronExpression and JobInput
// are not nil, and the cron string can be parsed.
func (u *UpdateOption) Valid() error {
	if u.CronExpression == nil && u.JobInput == nil {
		return ErrUpdateOptionInvalid
	}
	if _, err := u.schedule(); err != nil {
		return err
	}
	return nil
}

// getIdsFromUpdateScheduleList basically does schedules.map(rawJob -> rawJob.id)
func getIdsFromUpdateScheduleList(schedules []UpdateOption) []int64 {
	ids := make([]int64, len(schedules))
	for i, test := range schedules {
		ids[i] = test.JobID
	}
	return ids
}

// CronJobInput is the input passed to the Run function.
type CronJobInput struct {
	// JobID is the ID of the current job.
	JobID int64
	// GroupID is the GroupID of the current job.
	GroupID int64
	// SuperGroupID is the SuperGroupID of the current job.
	SuperGroupID int64
	// CronExpression is the interval of execution, as specified on job creation.
	CronExpression string
	// OtherInputs contains the other inputs of the job.
	OtherInputs map[string]interface{}
}

// CronJob is the interface jobs have to implement.
// It contains only one single method, `Run`.
type CronJob interface {
	Run(input CronJobInput)
}
