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
	expectedRaw  Job
}

func (j *jobToRawTest) TestToRaw(t *testing.T) {
	rawBuilt, err := j.withSchedule.BuildJob()
	if err != nil {
		t.Errorf("Cannot build Job from JobWithSchedule")
	}
	if !reflect.DeepEqual(j.expectedRaw, rawBuilt) {
		t.Errorf("The build test is wrong. Got\n%+v\nExpected\n%+v\n", rawBuilt, j.expectedRaw)
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

	var bufferJob1 bytes.Buffer
	encoder := gob.NewEncoder(&bufferJob1)
	interfaceEncode(t, encoder, &TestCronJob{})

	var bufferInput1 bytes.Buffer
	encoder = gob.NewEncoder(&bufferInput1)
	if err := encoder.Encode(CronJobInput{
		JobID:        1,
		GroupID:      1,
		SuperGroupID: 1,
		OtherInputs: map[string]interface{}{
			"life": "it seems to fade away",
		},
	}); err != nil {
		t.Errorf("Cannot encode input: %s\n", err.Error())
		t.FailNow()
	}

	pairs := []jobToRawTest{
		{
			withSchedule: JobWithSchedule{
				job: Job{
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
			expectedRaw: Job{
				ID:                 1,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             1,
				EverySecond:        20,
				Paused:             false,
				CreatedAt:          now,
				UpdatedAt:          now,
				SerializedJob:      bufferJob1.Bytes(),
				SerializedJobInput: bufferInput1.Bytes(),
			},
		},
	}

	for _, pair := range pairs {
		pair.TestToRaw(t)
	}
}

//func TestJobFromRaw(t *testing.T){
//	raw := []Job{
//		{
//			ID: 1,
//			GroupID: 1,
//			SuperGroupID: 1,
//			CronID: 0,
//			EverySecond: 120,
//			SerializedJob: []byte{},
//		},
//	}
//}
