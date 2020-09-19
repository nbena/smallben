package smallben

import "gorm.io/gorm"

type Repository struct {
	db *gorm.DB
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
	return r.db.Table("tests").Where("user_evaluation_rule_id in ?", ids).Updates(map[string]bool{"paused": true}).Error
}

// Resume `rule`. This function just performs an update, it is responsible of the call
// to set the tests as not paused.
// if `setUnpaused` is `true`, then the db automatically set the tests to `paused=false`.
func (r *Repository) ResumeUserEvaluationRule(rules []UserEvaluationRule, setUnpaused bool) error {
	//return r.db.Model(rule.Tests[0]).Where(
	//	"user_evaluation_rule_id = ?", rule.Id).Updates(rule.Tests).Error
	err := r.db.Transaction(func(tx *gorm.DB) error {
		for _, rule := range rules {
			if err := r.db.Model(Test{}).Where("user_evaluation_rule_id in ?").Updates(
				rule.Tests).Updates(map[string]bool{"paused": false}).Error; err != nil {
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

func getIdsFromUserEvaluationRuleList(rules []UserEvaluationRule) []int {
	ids := make([]int, len(rules))
	for i, rule := range rules {
		ids[i] = rule.Id
	}
	return ids
}
