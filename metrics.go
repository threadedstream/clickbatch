package batcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	statusLabel = "status"

	statusOk      = "ok"
	statusFailure = "failure"

	namespace = "batch"
)

var (
	batchInsertSuccess = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "insert",
		Name:      "status",
		Help:      "the metric is used to display the status of a last batch insert operation (1 for success, 0 for failure)",
	}, []string{statusLabel})

	batchFlushSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "flush",
		Name:      "size",
		Help:      "the metric is used to display the size of batch flushed to storage",
	})
)
