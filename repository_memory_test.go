package smallben

var (
	// accessedMemory is indexed by the id of the cron job
	// it is just used to have a concrete job implementation
	// for this repository test.
	accessedMemory = make(map[int64]CronJobInput)
)

type TestRepositoryMemoryCronJob struct{}

func (t *TestRepositoryMemoryCronJob) Run(input CronJobInput) {
	accessed[input.JobID] = input
}
