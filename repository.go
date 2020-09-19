package smallben

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(connectionOptions *RepositoryOptions) (Repository, error) {
	db, err := gorm.Open(postgres.Open(connectionOptions.String()), &gorm.Config{})
	if err != nil {
		return Repository{}, err
	}
	return Repository{db: db}, nil
}

// Create a new UserEvaluationRule storing it within the database.
func (r *Repository) AddUserEvaluationRule(rules []UserEvaluationRule) error {
	// the create operation is already executed within a transaction
	// and all the child items.
	// It works for a list of items as well.
	return r.db.Create(rules).Error
}

// Return a UserEvaluationRule whose id is `ruleID`.
func (r *Repository) GetUserEvaluationRule(ruleID int) (UserEvaluationRule, error) {
	var rule UserEvaluationRule
	result := r.db.Where("id = ?", ruleID).First(&rule)
	return rule, result.Error
}

func (r *Repository) PauseUserEvaluationRules(rules []UserEvaluationRule) error {
	//return r.db.Model(rule.Tests[0]).Where(
	//	"user_evaluation_rule_id = ?", rule.Id).Updates(map[string]interface{}{"paused": true, "CronId": 0}).Error
	ids := getIdsFromUserEvaluationRuleList(rules)
	return r.db.Debug().Table("tests").Where("user_evaluation_rule_id in ?", ids).Updates(map[string]interface{}{"paused": true}).Error
}

// Resume `rule`. This function updates the whole Test of `rules`, *and* set `paused=false`.
func (r *Repository) ResumeUserEvaluationRule(rules []UserEvaluationRule) error {
	tests := flatTests(rules)
	err := r.db.Transaction(func(tx *gorm.DB) error {
		for _, test := range tests {
			err := r.db.Debug().Model(&test).Updates(map[string]interface{}{"cron_id": test.CronId, "paused": false}).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (r *Repository) deleteUserEvaluationRule(rule *UserEvaluationRule) error {
	result := r.db.Delete(rule, rule.Id)
	if err := result.Error; err != nil {
		return err
	}
	if result.RowsAffected != 1 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// Delete a UserEvaluationRule whose id is `ruleID`. It fails if the record has not been found.
func (r *Repository) DeleteUserEvaluationRuleByKey(ruleID int) error {
	rule := UserEvaluationRule{Id: ruleID}
	return r.deleteUserEvaluationRule(&rule)
}

// Delete a UserEvaluationRule.
func (r *Repository) DeleteUserEvaluationRule(rule *UserEvaluationRule) error {
	return r.deleteUserEvaluationRule(rule)
}

func (r *Repository) DeleteUserEvaluationRules(rulesID []int) error {
	var rules []UserEvaluationRule
	result := r.db.Delete(&rules, rulesID)
	if err := result.Error; err != nil {
		return err
	}
	if result.RowsAffected != int64(len(rulesID)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// Returns all the UserEvaluationRule to execute (i.e., `.test.paused = false`).
func (r *Repository) GetAllUserEvaluationRulesToExecute() ([]UserEvaluationRule, error) {
	var rules []UserEvaluationRule

	// corresponds to select * from user_evaluation_rules join test where uer.id
	// in (select id from user_evaluation_rules where id in (select user_evaluation_rule_id from tests
	// where paused = false))
	result := r.db.Debug().Preload("Tests").Where("id in (?)",
		r.db.Table("user_evaluation_rules").Select("id").Where("id in (?)",
			r.db.Table("tests").Select("user_evaluation_rule_id").Where("paused = false"))).Find(&rules).Error

	return rules, result
}

// Saves the Test of `rules`.
func (r *Repository) SetCronIdOf(rules []UserEvaluationRule) error {
	// flattening all the tests
	tests := flatTests(rules)
	err := r.db.Transaction(func(tx *gorm.DB) error {
		for _, test := range tests {
			err := r.db.Debug().Model(&test).Updates(map[string]interface{}{"cron_id": test.CronId, "paused": false}).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
	// and then perform a single update
	return err
}

func getIdsFromUserEvaluationRuleList(rules []UserEvaluationRule) []int {
	ids := make([]int, len(rules))
	for i, rule := range rules {
		ids[i] = rule.Id
	}
	return ids
}
