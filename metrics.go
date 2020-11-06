package smallben

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
)

type metrics struct {
	total     prometheus.Gauge
	notPaused prometheus.Gauge
	paused    prometheus.Gauge
}

// newMetrics returns a new set of metrics.
func newMetrics() metrics {
	return metrics{
		total: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "smallben",
			Subsystem: "scheduler",
			Name:      "jobs_total",
			Help:      "Number of total jobs memorized by small ben",
		}),
		notPaused: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "smallben",
			Subsystem: "scheduler",
			Name:      "jobs_not_paused",
			Help:      "Number of jobs scheduled for execution by small ben",
		}),
		paused: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "smallben",
			Subsystem: "scheduler",
			Name:      "jobs_paused",
			Help:      "Number of jobs not scheduled for execution by small ben",
		}),
	}
}

// fillMetrics freshly sets the metrics.
func (s *SmallBen) fillMetrics() error {
	totalJobs, err := s.repository.ListJobs(nil)
	if err != nil {
		return err
	}
	paused := false
	notPausedJobs, err := s.repository.ListJobs(&ListJobsOptions{Paused: &paused})
	if err != nil {
		return err
	}
	paused = true
	pausedJobs, err := s.repository.ListJobs(&ListJobsOptions{Paused: &paused})
	if err != nil {
		return err
	}
	s.metrics.total.Add(float64(len(totalJobs)))
	s.metrics.notPaused.Add(float64(len(notPausedJobs)))
	s.metrics.paused.Add(float64(len(pausedJobs)))
	return nil
}

// addJobs updates the metrics
// by adding `size` jobs, considering them
// as being scheduled for execution.
func (m *metrics) addJobs(size int) {
	m.total.Add(float64(size))
	m.notPaused.Add(float64(size))
}

// pauseJobs updates the metrics
// by pausing `size` jobs.
func (m *metrics) pauseJobs(size int) {
	m.notPaused.Sub(float64(size))
	m.paused.Add(float64(size))
}

// resumeJobs updates the metrics
// by resuming `size` jobs.
func (m *metrics) resumeJobs(size int) {
	m.paused.Sub(float64(size))
	m.notPaused.Add(float64(size))
}

// postDelete updates metrics after a delete operation.
// - beforeJobs is the list of jobs that were on the scheduler before removing them
// - requestedJobs is the list of jobs users requested to delete
func (m *metrics) postDelete(beforeJobs []cron.Entry, requestedJobs []RawJob) {
	// actually, we don't know if the deleted jobs
	// were running or not, so we to find out
	for _, oldJob := range beforeJobs {
		found := false
		for _, gotJob := range requestedJobs {
			// if found, then the job was
			// in execution
			if int64(oldJob.ID) == gotJob.CronID {
				found = true
				break
			}
		}
		// found, then the job was in running
		if found {
			m.notPaused.Dec()
		} else {
			// otherwise, it was paused
			m.paused.Dec()
		}
	}
	m.total.Sub(float64(len(requestedJobs)))
}
