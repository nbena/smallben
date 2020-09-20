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

func (s *Scheduler) AddUserEvaluationRule(rules []UserEvaluationRule) ([]UserEvaluationRule, error) {

	var collectedEntries []cron.EntryID
	var err error

	modifiedRules := rules

	defer func() {
		// if there are errors, then remove
		// any added entries
		if err != nil {
			for _, entry := range collectedEntries {
				s.cron.Remove(entry)
			}
		}
	}()

	// for each rule
	for i, rule := range rules {
		// compute the list of inputs for the function
		inputs := rule.toRunFunctionInput()
		// for each of the possible input
		for j, input := range inputs {
			var entryID cron.EntryID
			// add the entry to the scheduler
			entryID, err = s.cron.AddFunc(getCronSchedule(rule.Tests[j].EverySecond), func() {
				runFunction(input)
			})
			// we can return without worrying about spurious element
			// since we have the defer function removing any added element
			// from the scheduler
			if err != nil {
				return nil, err
			}
			// otherwise, append the entry to the list
			collectedEntries = append(collectedEntries, entryID)
			// and also, store it into the test
			modifiedRules[i].Tests[j].CronId = int(entryID)
		}
	}
	return modifiedRules, nil
}

func (s *Scheduler) AddTests(tests []Test) ([]Test, error) {

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
		entryID, err = s.cron.AddFunc(getCronSchedule(test.EverySecond), func() {
			runFunction(input)
		})
		if err != nil {
			return modifiedTests, err
		}
		// otherwise, append the entry id
		collectedEntries = append(collectedEntries, entryID)
		modifiedTests[i].CronId = int(entryID)
	}
	return modifiedTests, err
}

// DeleteUserEvaluationRules delete `rules` from the scheduler.
func (s *Scheduler) DeleteUserEvaluationRules(rules []UserEvaluationRule) {
	for _, rule := range rules {
		s.DeleteTests(rule.Tests)
	}
}

// DeleteTests remove `tests` from the scheduler.
func (s *Scheduler) DeleteTests(tests []Test) {
	for _, test := range tests {
		s.cron.Remove(cron.EntryID(test.CronId))
	}
}

func getCronSchedule(seconds int) string {
	return fmt.Sprintf("@every %ds", seconds)
}

type runFunctionInput struct {
	testID               int
	userEvaluationRuleId int
	userID               int
}

func runFunction(input runFunctionInput) {
	fmt.Printf("Im running\n")
}
