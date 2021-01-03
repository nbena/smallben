package smallben

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"
)

type jobToRawTest struct {
	withSchedule JobWithSchedule
	expectedRaw  RawJob
}

type jobFromRawTest struct {
	raw                  RawJob
	expectedWithSchedule JobWithSchedule
}

type jobRawToJobTest struct {
	raw         RawJob
	expectedJob Job
}

func (j *jobToRawTest) TestToRaw(t *testing.T) {
	rawBuilt, err := j.withSchedule.BuildJob()
	if err != nil {
		t.Errorf("Cannot build RawJob from JobWithSchedule")
	}

	if !reflect.DeepEqual(j.expectedRaw, rawBuilt) {
		t.Errorf("The build test is wrong. Got\n%+v\nExpected\n%+v\n", rawBuilt, j.expectedRaw)
	}
}

func (j *jobFromRawTest) TestFromRaw(t *testing.T) {
	withScheduleBuilt, err := j.raw.ToJobWithSchedule()
	if err != nil {
		t.Errorf("Cannot build JobWithSchedule from RawJob: %s\n", err.Error())
		t.FailNow()
	}
	// reflect.DeepEqual does not work very well
	if withScheduleBuilt.rawJob.ID != j.expectedWithSchedule.rawJob.ID {
		t.Errorf("ID is different. Got: %d, expected: %d\n",
			withScheduleBuilt.rawJob.ID, j.expectedWithSchedule.rawJob.ID)
	}
	if withScheduleBuilt.rawJob.GroupID != j.expectedWithSchedule.rawJob.GroupID {
		t.Errorf("GroupID is different. Got: %d, expected: %d\n",
			withScheduleBuilt.rawJob.ID, j.expectedWithSchedule.rawJob.ID)
	}
	if withScheduleBuilt.rawJob.SuperGroupID != j.expectedWithSchedule.rawJob.SuperGroupID {
		t.Errorf("SuperGroudID is different. Got: %d, expected: %d\n",
			withScheduleBuilt.rawJob.SuperGroupID, j.expectedWithSchedule.rawJob.SuperGroupID)
	}
	if withScheduleBuilt.rawJob.CronID != j.expectedWithSchedule.rawJob.CronID {
		t.Errorf("CronID is different. Got: %d, expected: %d\n",
			withScheduleBuilt.rawJob.CronID, j.expectedWithSchedule.rawJob.CronID)
	}
	if withScheduleBuilt.rawJob.CronExpression != j.expectedWithSchedule.rawJob.CronExpression {
		t.Errorf("CronExpression is different. Got: %s, expected: %s\n",
			withScheduleBuilt.rawJob.CronExpression, j.expectedWithSchedule.rawJob.CronExpression)
	}
	if withScheduleBuilt.rawJob.Paused != j.expectedWithSchedule.rawJob.Paused {
		t.Errorf("Paused is different. Got: %v, expected: %v\n",
			withScheduleBuilt.rawJob.Paused, j.expectedWithSchedule.rawJob.Paused)
	}
	if !withScheduleBuilt.rawJob.CreatedAt.Equal(j.expectedWithSchedule.rawJob.CreatedAt) {
		t.Errorf("CreatedAt is different. God:\n%v\nExpected:\n%v\n",
			withScheduleBuilt.rawJob.CreatedAt, j.expectedWithSchedule.rawJob.CreatedAt)
	}
	if !withScheduleBuilt.rawJob.UpdatedAt.Equal(j.expectedWithSchedule.rawJob.UpdatedAt) {
		t.Errorf("UpdatedAt is different. God:\n%v\nExpected:\n%v\n",
			withScheduleBuilt.rawJob.UpdatedAt, j.expectedWithSchedule.rawJob.UpdatedAt)
	}
	if !reflect.DeepEqual(withScheduleBuilt.runInput, j.expectedWithSchedule.runInput) {
		t.Errorf("runInput is different. Got:\n%+v\nExpected:\n%+v\n",
			withScheduleBuilt.runInput, j.expectedWithSchedule.runInput)
	}
}

func (j *jobRawToJobTest) TestJobRawToJob(t *testing.T) {
	built, err := j.raw.toJob()
	if err != nil {
		t.Errorf("Fail to build Job: %s\n", err.Error())
		t.FailNow()
	}
	if !reflect.DeepEqual(built, j.expectedJob) {
		t.Errorf("The toJob is wrong. Got\n%+v\nExpected\n%+v\n", built, j.expectedJob)
		t.FailNow()
	}
	// also, we test the UpdatedAt() and CreatedAt() just
	// to increase coverage since there are no other ways
	// where job.Created() and job.UpdatedAt() are called
	if built.CreatedAt() != j.expectedJob.CreatedAt() {
		t.Errorf("CreatedAt mismatch. Got: %s, expected: %s\n",
			built.CreatedAt().String(), j.expectedJob.CreatedAt().String())
	}
	if built.UpdatedAt() != j.expectedJob.UpdatedAt() {
		t.Errorf("UpdatedAt mismatch. Got: %s, expected: %s\n",
			built.UpdatedAt().String(), j.expectedJob.UpdatedAt().String())
	}
	// do the same for paused
	if built.Paused() != j.expectedJob.Paused() {
		t.Errorf("Paused mismatch. Got: %v, expected: %v\n",
			built.Paused(), j.expectedJob.Paused())
	}
}

// interfaceEncode encodes `job` into `encoder`.
func interfaceEncode(encoder *gob.Encoder, job CronJob, t *testing.T) {
	if err := encoder.Encode(&job); err != nil {
		t.Errorf("Fail to gob encode: %s\n", err.Error())
		t.FailNow()
	}
}

// inputEncode encodes `data` in a json string.
func inputEncode(data map[string]interface{}, t *testing.T) string {
	encodedData, err := json.Marshal(data)
	if err != nil {
		t.Errorf("Fail to json encode: %s\n", err.Error())
		t.FailNow()
	}
	return string(encodedData)
}

func TestJobToRaw(t *testing.T) {
	now := time.Now()

	inputJob1 := CronJobInput{
		JobID:          1,
		GroupID:        1,
		SuperGroupID:   1,
		CronExpression: "@every 20s",
		OtherInputs: map[string]interface{}{
			"life": "it seems to fade away",
		},
	}

	jobSerialized1, inputSerialized1 := fakeSerialized(t, inputJob1.OtherInputs)

	pairs := []jobToRawTest{
		{
			withSchedule: JobWithSchedule{
				rawJob: RawJob{
					ID:                 1,
					GroupID:            1,
					SuperGroupID:       1,
					CronID:             1,
					CronExpression:     "@every 20s",
					Paused:             false,
					CreatedAt:          now,
					UpdatedAt:          now,
					SerializedJob:      "",
					SerializedJobInput: "",
				},
				schedule: scheduleNeverFail(t, 20),
				run:      &TestCronJobNoop{},
				runInput: CronJobInput{
					JobID:        1,
					GroupID:      1,
					SuperGroupID: 1,
					OtherInputs: map[string]interface{}{
						"life": "it seems to fade away",
					},
				},
			},
			expectedRaw: RawJob{
				ID:                 1,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             1,
				CronExpression:     "@every 20s",
				Paused:             false,
				CreatedAt:          now,
				UpdatedAt:          now,
				SerializedJob:      jobSerialized1,
				SerializedJobInput: inputSerialized1,
			},
		},
	}

	for _, pair := range pairs {
		pair.TestToRaw(t)
	}
}

func TestJobRawToJob(t *testing.T) {
	now := time.Now()

	inputJob1 := CronJobInput{
		JobID:          1,
		GroupID:        1,
		SuperGroupID:   1,
		CronExpression: "@every 1s",
		OtherInputs: map[string]interface{}{
			"life": "it seems to fade away",
		},
	}

	jobSerialized1, inputSerialized1 := fakeSerialized(t, inputJob1.OtherInputs)

	pairs := []jobRawToJobTest{
		{
			raw: RawJob{
				ID:                 1,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             0,
				CronExpression:     "@every 1s",
				Paused:             false,
				CreatedAt:          now,
				UpdatedAt:          now,
				SerializedJob:      jobSerialized1,
				SerializedJobInput: inputSerialized1,
			},
			expectedJob: Job{
				ID:             1,
				GroupID:        1,
				SuperGroupID:   1,
				cronID:         0,
				CronExpression: "@every 1s",
				paused:         false,
				createdAt:      now,
				updatedAt:      now,
				Job:            &TestCronJobNoop{},
				JobInput: map[string]interface{}{
					"life": "it seems to fade away",
				},
			},
		},
	}

	for _, pair := range pairs {
		pair.TestJobRawToJob(t)
	}
}

func TestJobFromRawWithError(t *testing.T) {
	raw := RawJob{
		CronExpression:     "@every 1s",
		SerializedJob:      "",
		SerializedJobInput: "",
	}
	_, err := raw.ToJobWithSchedule()
	checkErrorIsOf(err, io.EOF, t)

	// now, set the first one to a valid job
	jobSerialized, _ := fakeSerialized(t, map[string]interface{}{})
	// passing in an invalid json-encoded input
	raw.SerializedJob = jobSerialized
	_, err = raw.ToJobWithSchedule()
	checkErrorMsg(err, "unexpected end of JSON input", t)

	raw.CronExpression = "not a valid cron expression"
	_, err = raw.ToJobWithSchedule()
	if err == nil {
		t.Errorf("An invalid schedule has been accepted")
		t.FailNow()
	}

	job := Job{
		CronExpression: "not a valid cron expression",
	}
	_, err = job.toJobWithSchedule()
	if err == nil {
		t.Errorf("An invalid schedule has been accepted")
		t.FailNow()
	}

	// set a non valid job
	raw.SerializedJob = "not a valid CronJob"
	_, err = raw.toJob()
	if err == nil {
		t.Errorf("A not valid CronJob has been decoded")
		t.FailNow()
	}

	// set a non valid job input
	// first, we need a valid job input
	raw.SerializedJob = jobSerialized
	raw.SerializedJobInput = "not a valid job input"
	_, err = raw.toJob()
	if err == nil {
		t.Errorf("A not valid CronJob has been decoded")
		t.FailNow()
	}

}

func TestJobFromRaw(t *testing.T) {
	now := time.Now()

	inputJob1 := CronJobInput{
		JobID:          1,
		GroupID:        1,
		SuperGroupID:   1,
		CronExpression: "@every 1s",
		OtherInputs: map[string]interface{}{
			"life": "it seems to fade away",
		},
	}

	jobSerialized1, inputSerialized1 := fakeSerialized(t, inputJob1.OtherInputs)

	pairs := []jobFromRawTest{
		{
			raw: RawJob{
				ID:                 1,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             1,
				CronExpression:     "@every 1s",
				Paused:             false,
				CreatedAt:          now,
				UpdatedAt:          now,
				SerializedJob:      jobSerialized1,
				SerializedJobInput: inputSerialized1,
			},
			expectedWithSchedule: JobWithSchedule{
				rawJob: RawJob{
					ID:                 1,
					GroupID:            1,
					SuperGroupID:       1,
					CronID:             1,
					CronExpression:     "@every 1s",
					Paused:             false,
					CreatedAt:          now,
					UpdatedAt:          now,
					SerializedJob:      "",
					SerializedJobInput: "",
				},
				schedule: scheduleNeverFail(t, 1),
				run:      &TestCronJobNoop{},
				runInput: inputJob1,
			},
		},
	}

	for _, pair := range pairs {
		pair.TestFromRaw(t)
	}
}

// fakeSerialized serializes a Job of type TestCronJobNoop
// and its input returning them as encoded strings.
func fakeSerialized(t *testing.T, input map[string]interface{}) (string, string) {
	var bufferJob bytes.Buffer

	encoder := gob.NewEncoder(&bufferJob)
	testCronJob := &TestCronJobNoop{}

	interfaceEncode(encoder, testCronJob, t)

	jobInputEncoded := inputEncode(input, t)

	return base64.StdEncoding.EncodeToString(bufferJob.Bytes()), jobInputEncoded
}

func TestJobUpdateValid(t *testing.T) {
	invalid := UpdateOption{}
	checkErrorIsOf(invalid.Valid(), ErrUpdateOptionInvalid, t)
	invalid.CronExpression = stringPointer("@@")
	checkErrorMsg(invalid.Valid(), "unrecognized descriptor: @@", t)
}
