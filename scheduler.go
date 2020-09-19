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
			entryID, err = s.cron.AddFunc(getCronSchedule(rule.Tests[j].Seconds), func() {
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

func (s *Scheduler) DeleteUserEvaluationRules(rules []UserEvaluationRule) {
	for _, rule := range rules {
		for _, test := range rule.Tests {
			s.cron.Remove(cron.EntryID(test.Id))
		}
	}
}

func getCronSchedule(seconds int) string {
	return fmt.Sprintf("@every %d", seconds)
}

type runFunctionInput struct {
	testID               int
	userEvaluationRuleId int
	userID               int
}

func runFunction(input runFunctionInput) {
	fmt.Printf("Im running\n")
}
