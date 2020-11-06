package smallben

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	total     prometheus.Gauge
	notPaused prometheus.Gauge
	paused    prometheus.Gauge
}

// newMetrics returns a new set of metrics.
func newMetrics() metrics {
	return metrics{
		total: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "smallben",
			Subsystem: "scheduler",
			Name:      "jobs_total",
			Help:      "Number of total jobs memorized by small ben",
		}),
		notPaused: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "smallben",
			Subsystem: "scheduler",
			Name:      "jobs_not_paused",
			Help:      "Number of jobs scheduled for execution by small ben",
		}),
		paused: promauto.NewGauge(prometheus.GaugeOpts{
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
