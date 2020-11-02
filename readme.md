# SmallBen

`SmallBen` is a small and simple **persistent scheduling library**, that basically combines [cron](https://github.com/robfig/cron) and a persistence layer. That means that jobs that are added to the scheduler will persist across runs. As of now, the only supported persistence layer is [gorm](https://gorm.io/).

Features:

- **simple**, both to use and to maintain
- relies on **well-known** libraries, it just adds a thin layer on top of them

This library can be thought, somehow, as a (much) simpler version of Java [quartz](http://www.quartz-scheduler.org/).

## Jobs

A `Job` is the very central `struct` of this library. A `Job` contains, among the others, the following fields, which must be specified by the user.

- `ID`: unique identifier of each job
- `GroupID`: unique identifier useful to group jobs together
- `SuperGroupID`: unique identifier useful to group groups of jobs together. For instance, it can be used to model different users. The semantic is left to the user.

Depending on the underlying storage, the `ID` might be unique together in each `GroupID`, and the same might applies for `GroupID` within `SuperGroupID`.

The concrete execution logic of a `Job` is wrapped in the `CronJob` interface, which is defined as follows.

```go
// CronJob is the interface jobs have to implement.
// It contains only one single method, `Run`.
type CronJob interface {
	Run(input CronJobInput)
}
```

The `Run` method takes input a `struct` of type `CronJobInput`, which is defined as follows.

```go
// CronJobInput is the input passed to the Run function.
type CronJobInput struct {
	JobID        int64
	GroupID      int64
	SuperGroupID int64
	OtherInputs  map[string]interface{}
}
```

In practice, each (implementation of) `CronJob` receives in input a buch of data containing some information about the job itself. In particular, `OtherInputs` is a map that can contain arbitrary data needed for the job.

Since they are persisted using `gob` serialization, it is important to:

- **register** the concrete types implementing `CronJob` (see below)
- pay **attention** to updates to the code, since they might break serialization/deserialization.

 ## Examples
 
 The first thing to do is to **configure the persistent storage**. 
 
 The `gorm`-backed storage is called `RepositoryGorm`, and is created by passing in two structs:
 
 - `gorm.Dialector`
 - `gorm.Config`
 
 ```go
import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"    
)

dialector := []gorm.Dialector{
    postgres.Open("host=localhost dbname=postgres port=5432 user=postgres password=postgres")
}
repo, _ := NewRepositoryGorm(&RepositoryGormConfig{
    Dialector: dialector,
    Config: gorm.Config{}.
})
```

The second thing to do is to define an implementation of the `CronJob` interface.

```go
import (
    "fmt"
    "github.com/nbena/smallben"
)

type FooJob struct {}

func(f *FooJob) Run(input smallben.CronJobInput) {
    fmt.Printf("You are calling me, my ID is: %d, my GroupID is: %d, my SuperGroupID is: %d\n",
        input.JobID, input.GroupID, input.SuperGroupID)
}
```

Now, this implementation must be registered, to make `gob` encoding work. A good place to do it is in the `init()` function.

```go
import (
    "encoding/gob"
)

func init() {
    gob.Register(&FooJob{})
}
```

The third thing to do is to actually create a `Job`, which we later submit to `SmallBen`. Other than `ID`, `GroupID` and `SuperGroupID`, the following field must be specified.

- `CronExpression` to specify the execution interval, following the format used by [cron](https://github.com/robfig/cron/v3)
- `Job` to specify the actual implementation of `CronJob` to execute
- `JobInput` to specify other inputs to pass to the `CronJob` implementation. They will be available at `input.OtherInputs`, and they are **static**, i.e., each modification to them is **not persisted**. 

```go
import (
    "github.com/nbena/smallben"
)

job := smallben.Job{
    ID: 1,
    GroupID: 1,
    SuperGroupID: 1,
    // executed every 5 seconds
    CronExpression: "@every 5s",
    Job: &FooJob{},
    JobInput: make(map[string]interface{}),
}
```

The fourth thing to do is to actually create the scheduler. It receives just one parameter, the previously created storage.

```go
// create
scheduler := smallben.New(repo)
```

Next, the scheduler must be started. Starting the scheduler will make it fetching all the `Job` within the storage that must be executed.

```go
err := scheduler.Start()
```

Add this point, our `Job` can be added to the scheduler. All operations are done in batches.

```go
err := scheduler.AddJobs([]Job{job})
```

That's all.