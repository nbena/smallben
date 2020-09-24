package smallben

import (
	"bytes"
	"encoding/gob"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type jobFromRawTest struct {
	rawJobs     Job
	expectedJob JobWithSchedule
}

type JobFromRawTestSuite struct {
	suite.Suite
	raw      []Job
	expected []JobWithSchedule
}

func (s *JobFromRawTestSuite) TestToJobWithSchedule() {
	for i, raw := range s.raw {
		built, err := raw.ToJobWithSchedule()
		s.Nil(err)
		s.Equal(s.expected[i], built)
	}
}

type jobToRawTest struct {
	withSchedule JobWithSchedule
	expectedRaw  Job
}

// Test the BuildJob() method
type JobToRawTestSuite struct {
	suite.Suite
	pairs []jobToRawTest
}

func (j *JobToRawTestSuite) TestToRaw() {
	for _, test := range j.pairs {
		rawBuilt, err := test.withSchedule.BuildJob()
		j.Nil(err)
		j.Equal(test.expectedRaw, rawBuilt)
	}
}

func interfaceEncode(t *testing.T, encoder *gob.Encoder, job CronJob) {
	if err := encoder.Encode(&job); err != nil {
		t.Errorf("Fail to encode: %s\n", err.Error())
		t.FailNow()
	}
}

func TestJobToRaw(t *testing.T) {
	testSuite := new(JobToRawTestSuite)

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
					serializedJob:      []byte{},
					serializedJobInput: []byte{},
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
				serializedJob:      bufferJob1.Bytes(),
				serializedJobInput: bufferInput1.Bytes(),
			},
		},
	}
	testSuite.pairs = pairs
	suite.Run(t, testSuite)
}

//func TestJobFromRaw(t *testing.T){
//	raw := []Job{
//		{
//			ID: 1,
//			GroupID: 1,
//			SuperGroupID: 1,
//			CronID: 0,
//			EverySecond: 120,
//			serializedJob: []byte{},
//		},
//	}
//}
