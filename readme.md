# SmallBen

`SmallBen` is a small and simple **persistent scheduling library**, that basically combines [cron](https://github.com/robfig/cron) and a persistence layer. That means that jobs that are added to the scheduler will persist across runs. As of now, the only supported persistence layer is [gorm](https://gorm.io/).

Features:

- **simple**, both to use and to maintain
- relies on **well-known** libraries, it just adds a thin layer on top of them

This library can be thought, somehow, as a (much) simpler version of Java [quartz](http://www.quartz-scheduler.org/).

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

The second thing to do is to **register the job structs**. In fact, all the jobs the scheduler execute implement the interface `CronJob`, defined as follows.

```go
// CronJob is the interface jobs have to implement.
// It contains only one single method, `Run`.
type CronJob interface {
	Run(input CronJobInput)
}
```
