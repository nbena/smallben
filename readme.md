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
type FooJob struct {}

func(f *FooJob) Run(input CronJobInput) {
}
```