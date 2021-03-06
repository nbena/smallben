package smallben

import (
	"reflect"
	"testing"
)

// stringPointer returns a pointer to the passed in value.
func stringPointer(value string) *string {
	return &value
}

type testPauseResumeOptions struct {
	option   PauseResumeOptions
	expected ListJobsOptions
}

func (o *testPauseResumeOptions) testPauseResumeOptionToListJob(t *testing.T) {
	built := o.option.toListOptions()
	if !reflect.DeepEqual(built, o.expected) {
		t.Errorf("Built failed. Got\n%+v\nExpected\n%+v\n", built, o.expected)
	}
}

func TestPauseResumeOptionToListJob(t *testing.T) {
	pairs := []testPauseResumeOptions{
		{
			option: PauseResumeOptions{
				JobIDs: []int64{100},
			},
			expected: ListJobsOptions{
				JobIDs: []int64{100},
			},
		},
	}

	for _, pair := range pairs {
		pair.testPauseResumeOptionToListJob(t)
	}
}

type testDeleteOptions struct {
	option   DeleteOptions
	expected ListJobsOptions
}

func (o *testDeleteOptions) test(t *testing.T) {
	built := o.option.toListOptions()
	if !reflect.DeepEqual(built, o.expected) {
		t.Errorf("Built failed. Got\n%+v\nExpected\n%+v\n", built, o.expected)
	}
}

func TestDeleteOptionToListJob(t *testing.T) {
	pairs := []testDeleteOptions{
		{
			option: DeleteOptions{
				PauseResumeOptions: PauseResumeOptions{
					JobIDs: []int64{100},
				},
			},
			expected: ListJobsOptions{
				JobIDs: []int64{100},
			},
		},
		{
			option: DeleteOptions{
				PauseResumeOptions: PauseResumeOptions{
					GroupIDs: []int64{100},
				},
			},
			expected: ListJobsOptions{
				GroupIDs: []int64{100},
			},
		},
	}

	for _, pair := range pairs {
		pair.test(t)
	}
}
