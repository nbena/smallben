package smallben

import (
	"fmt"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cron *cron.Cron
}

// Returns a new Scheduler.
func NewScheduler() Scheduler {
	return Scheduler{
		cron: cron.New(cron.WithSeconds()),
	}
}

// AddTests2 adds `test` to the scheduler. This function never fails and updates
// the input array with the `CronID`.
func (s *Scheduler) AddTests2(tests []JobWithSchedule) {

	for _, test := range tests {
		job := test.toRunFunctionInput()
		entryID := s.cron.Schedule(test.schedule, job)
		test.CronID = int32(entryID)
	}
}

// DeleteTestsWithSchedule remove `tests` from the scheduler. This function never fails.
func (s *Scheduler) DeleteTestsWithSchedule(tests []JobWithSchedule) {
	for _, test := range tests {
		s.cron.Remove(cron.EntryID(test.CronID))
	}
}

func (s *Scheduler) AddTests(tests []Job) ([]Job, error) {

	var collectedEntries []cron.EntryID
	var err error

	modifiedTests := tests

	defer func() {
		// if there are errors, then remove
		// any added entries
		if err != nil {
			for _, entry := range collectedEntries {
				s.cron.Remove(entry)
			}
		}
	}()

	// for each test
	for i, test := range tests {
		input := test.toRunFunctionInput()
		var entryID cron.EntryID
		entryID, err = s.cron.AddFunc(getCronSchedule(int(test.EverySecond)), func() {
			input.Run()
		})
		if err != nil {
			return modifiedTests, err
		}
		// otherwise, append the entry id
		collectedEntries = append(collectedEntries, entryID)
		modifiedTests[i].CronID = int32(entryID)
	}
	return modifiedTests, err
}

// DeleteTests remove `tests` from the scheduler.
func (s *Scheduler) DeleteTests(tests []Job) {
	for _, test := range tests {
		s.cron.Remove(cron.EntryID(test.CronID))
	}
}

func getCronSchedule(seconds int) string {
	return fmt.Sprintf("@every %ds", seconds)
}

type runFunctionInput struct {
	jobID        int32
	groupID      int32
	superGroupID int32
}

func (r *runFunctionInput) Run() {
	fmt.Printf("Im running\n")
}
