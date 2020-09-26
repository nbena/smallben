package smallben

import (
	"bytes"
	"encoding/gob"
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
		t.Errorf("Cannot build JobWithSchedule from RawJob")
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
	if withScheduleBuilt.rawJob.EverySecond != j.expectedWithSchedule.rawJob.EverySecond {
		t.Errorf("EverySecond is different. Got: %d, expected: %d\n",
			withScheduleBuilt.rawJob.EverySecond, j.expectedWithSchedule.rawJob.EverySecond)
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

func interfaceEncode(t *testing.T, encoder *gob.Encoder, job CronJob) {
	if err := encoder.Encode(&job); err != nil {
		t.Errorf("Fail to encode: %s\n", err.Error())
		t.FailNow()
	}
}

func TestJobToRaw(t *testing.T) {
	now := time.Now()

	inputJob1 := CronJobInput{
		JobID:        1,
		GroupID:      1,
		SuperGroupID: 1,
		OtherInputs: map[string]interface{}{
			"life": "it seems to fade away",
		},
	}

	jobSerialized1, inputSerialized1 := fakeSerialized(t, inputJob1)

	pairs := []jobToRawTest{
		{
			withSchedule: JobWithSchedule{
				rawJob: RawJob{
					ID:                 1,
					GroupID:            1,
					SuperGroupID:       1,
					CronID:             1,
					EverySecond:        20,
					Paused:             false,
					CreatedAt:          now,
					UpdatedAt:          now,
					SerializedJob:      []byte{},
					SerializedJobInput: []byte{},
				},
				schedule: scheduleNeverFail(t, 20),
				run:      &TestCronJob{},
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
				EverySecond:        20,
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

func TestJobFromRaw(t *testing.T) {
	now := time.Now()

	inputJob1 := CronJobInput{
		JobID:        1,
		GroupID:      1,
		SuperGroupID: 1,
		OtherInputs: map[string]interface{}{
			"life": "it seems to fade away",
		},
	}

	jobSerialized1, inputSerialized1 := fakeSerialized(t, inputJob1)

	pairs := []jobFromRawTest{
		{
			raw: RawJob{
				ID:                 1,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             1,
				EverySecond:        1,
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
					EverySecond:        1,
					Paused:             false,
					CreatedAt:          now,
					UpdatedAt:          now,
					SerializedJob:      []byte{},
					SerializedJobInput: []byte{},
				},
				schedule: scheduleNeverFail(t, 1),
				run:      &TestCronJob{},
				runInput: inputJob1,
			},
		},
	}

	for _, pair := range pairs {
		pair.TestFromRaw(t)
	}
}

// serialize a input for us
func fakeSerialized(t *testing.T, input CronJobInput) ([]byte, []byte) {
	var bufferJob bytes.Buffer
	var bufferInput bytes.Buffer

	encoder := gob.NewEncoder(&bufferJob)
	interfaceEncode(t, encoder, &TestCronJob{})

	encoder = gob.NewEncoder(&bufferInput)
	if err := encoder.Encode(input); err != nil {
		t.Errorf("Cannot encode input: %s\n", err.Error())
		t.FailNow()
	}

	return bufferJob.Bytes(), bufferInput.Bytes()
}
