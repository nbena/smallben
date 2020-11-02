# SmallBen

`SmallBen` is a small and simple **persistent scheduling library**, that basically combines [cron](https://github.com/robfig/cron) and a persistence layer. That means that jobs that are added to the scheduler will persist across runs. As of now, the only supported persistence layer is [gorm](https://gorm.io/).

Features:

- **simple**, both to use and to maintain
- relies on **well-known** libraries, it just adds a thin layer on top of them 