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

func (r *Repository) PauseUserEvaluationRule(rule *UserEvaluationRule) error {
	return r.db.Model(rule.Tests[0]).Where(
		"user_evaluation_rule_id = ?", rule.Id).Updates(map[string]interface{}{"paused": true, "CronId": 0}).Error
}

// Resume `rule`. This function just performs an update, it is responsible of the call
// to set the tests as not paused.
func (r *Repository) ResumeUserEvaluationRule(rule *UserEvaluationRule) error {
	return r.db.Model(rule.Tests[0]).Where(
		"user_evaluation_rule_id = ?", rule.Id).Updates(rule.Tests).Error
}

func (r *Repository) deleteUserEvaluationRule(rule * UserEvaluationRule) error {
	result := r.db.Delete(rule, rule.Id)
	if err:= result.Error; err != nil {
		return err
	}
	if result.RowsAffected != 1 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// Delete a UserEvaluationRule whose id is `ruleID`. It fails if the record has not been found.
func(r *Repository) DeleteUserEvaluationRuleByKey(ruleID int) error {
	rule := UserEvaluationRule{Id: ruleID}
	return r.deleteUserEvaluationRule(&rule)
}

// Delete a UserEvaluationRule.
func(r *Repository) DeleteUserEvaluationRule(rule *UserEvaluationRule) error {
	return r.deleteUserEvaluationRule(rule)
}

func (r *Repository) DeleteUserEvaluationRules(rulesID []int) error {
	var rules []UserEvaluationRule
	result := r.db.Delete(&rules, rulesID)
	if err:= result.Error; err != nil {
		return err
	}
	if result.RowsAffected != int64(len(rulesID)) {
		return gorm.ErrRecordNotFound
	}
	return nil
}