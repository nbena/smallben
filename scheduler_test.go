package smallben

import (
	"github.com/robfig/cron/v3"
	"sync"
	"testing"
	"time"
)

type SchedulerTestSuite struct {
	scheduler Scheduler
	jobs      []JobWithSchedule
}

var lockMap = new(LockMap)

type SchedulerTestCronJob struct{}

func (s *SchedulerTestCronJob) Run(input CronJobInput) {
	lockMap.lock.Lock()
	defer lockMap.lock.Unlock()
	lockMap.counter += input.OtherInputs["counter"].(int)
}

type LockMap struct {
	counter int
	lock    sync.Mutex
}

func (s *SchedulerTestSuite) TestAdd(t *testing.T) {
	lenOfJobsBeforeAnything := len(s.scheduler.cron.Entries())

	// add the jobs
	s.scheduler.AddJobs(s.jobs)

	// make sure they have been added
	lenOfJobsAfterFirstAdd := len(s.scheduler.cron.Entries())
	if lenOfJobsAfterFirstAdd != lenOfJobsBeforeAnything+len(s.jobs) {
		t.Errorf("Fail to add jobs. Got: %d Expected: %d\n", lenOfJobsAfterFirstAdd, lenOfJobsBeforeAnything+len(s.jobs))
	}

	// make sure they have been executed, so we
	// compute the expected sum
	// it is the lower bound
	expectedSum := 0
	for _, job := range s.jobs {
		expectedSum += job.runInput.OtherInputs["counter"].(int)
	}

	// sleep fot the necessary time.
	time.Sleep(2 * time.Second)
	lockMap.lock.Lock()
	if lockMap.counter < expectedSum {
		t.Errorf("The counter has not been incremented properly. Got: %d Expected: %d\n",
			lockMap.counter, expectedSum)
	}
	lockMap.lock.Unlock()

	// ok, now remove the entries
	s.scheduler.DeleteJobsWithSchedule(s.jobs)

	// and make sure they have been removed
	lenOfJobsAfterFirstRemove := len(s.scheduler.cron.Entries())
	if lenOfJobsAfterFirstRemove != lenOfJobsBeforeAnything {
		t.Errorf("Some jobs have not been removed properly. Got: %d Expected: %d\n",
			lenOfJobsAfterFirstRemove, lenOfJobsBeforeAnything)
	}

	// now, re-add, in order to test the other delete method
	s.scheduler.AddJobs(s.jobs)
	// now we need to build the array for raw jobs
	rawJobs := make([]RawJob, len(s.jobs))
	for i := range s.jobs {
		rawJobs[i] = s.jobs[i].rawJob
	}
	s.scheduler.DeleteJobs(rawJobs)
	lenOfJobsAfterSecondRemove := len(s.scheduler.cron.Entries())
	if lenOfJobsAfterSecondRemove != lenOfJobsBeforeAnything {
		t.Errorf("Some jobs have not been removed properly. Got: %d Expected: %d\n",
			lenOfJobsAfterSecondRemove, lenOfJobsBeforeAnything)
	}
}

func (s *SchedulerTestSuite) setup() {
	s.scheduler = NewScheduler()
	s.scheduler.cron.Start()

	s.jobs = []JobWithSchedule{
		{
			rawJob: RawJob{
				ID:                 1,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             0,
				CronExpression:     "@every 1s",
				Paused:             false,
				CreatedAt:          time.Now(),
				UpdatedAt:          time.Now(),
				SerializedJob:      nil,
				SerializedJobInput: nil,
			},
			run: &SchedulerTestCronJob{},
			runInput: CronJobInput{
				JobID:        1,
				GroupID:      1,
				SuperGroupID: 1,
				OtherInputs: map[string]interface{}{
					"counter": 10,
				},
			},
			schedule: cron.ConstantDelaySchedule{Delay: 1 * time.Second},
		},
		{
			rawJob: RawJob{
				ID:                 2,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             0,
				CronExpression:     "@every 1s",
				Paused:             false,
				CreatedAt:          time.Now(),
				UpdatedAt:          time.Now(),
				SerializedJob:      nil,
				SerializedJobInput: nil,
			},
			run: &SchedulerTestCronJob{},
			runInput: CronJobInput{
				JobID:        2,
				GroupID:      1,
				SuperGroupID: 1,
				OtherInputs: map[string]interface{}{
					"counter": 1,
				},
			},
			schedule: cron.ConstantDelaySchedule{Delay: 1 * time.Second},
		},
		{
			rawJob: RawJob{
				ID:                 3,
				GroupID:            1,
				SuperGroupID:       1,
				CronID:             0,
				CronExpression:     "@every 1s",
				Paused:             false,
				CreatedAt:          time.Now(),
				UpdatedAt:          time.Now(),
				SerializedJob:      nil,
				SerializedJobInput: nil,
			},
			run: &SchedulerTestCronJob{},
			runInput: CronJobInput{
				JobID:        3,
				GroupID:      1,
				SuperGroupID: 1,
				OtherInputs: map[string]interface{}{
					"counter": 3,
				},
			},
			schedule: cron.ConstantDelaySchedule{Delay: 1 * time.Second},
		},
	}
}

func (s *SchedulerTestSuite) teardown() {
	s.scheduler.DeleteJobsWithSchedule(s.jobs)
	s.scheduler.cron.Stop()
}

func TestScheduler(t *testing.T) {
	test := new(SchedulerTestSuite)
	test.setup()
	test.TestAdd(t)
	test.teardown()
}
