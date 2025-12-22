package exporter

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// metricAggregator collects metrics for service-level aggregation
type metricAggregator struct {
	mu      sync.Mutex
	metrics map[aggregationKey]*aggregatedValues
}

type aggregationKey struct {
	cluster       string
	componentType string
	namespace     string
	metricName    string
}

type aggregatedValues struct {
	sum   float64
	count int
	max   float64
}

func newMetricAggregator() *metricAggregator {
	return &metricAggregator{
		metrics: make(map[aggregationKey]*aggregatedValues),
	}
}

// Metrics we want to aggregate
var aggregatedMetrics = map[string]struct{}{
	"nebula_graphd_query_latency_us_p99_60": {},
	"nebula_graphd_num_query_errors_sum_60": {},
	"nebula_graphd_num_queries_sum_60":      {},
}

// addRaw adds a metric using raw data (before Prometheus metric is created)
// This is cleaner than parsing Prometheus metric descriptors
func (a *metricAggregator) addRaw(metricName string, value float64, componentType, cluster, namespace string) {
	// Only aggregate specific metrics
	if _, ok := aggregatedMetrics[metricName]; !ok {
		return
	}

	// Only aggregate graphd metrics
	if componentType != ComponentGraphdLabelVal {
		return
	}

	key := aggregationKey{
		cluster:       cluster,
		componentType: componentType,
		namespace:     namespace,
		metricName:    metricName,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.metrics[key]; !exists {
		a.metrics[key] = &aggregatedValues{
			sum:   value,
			count: 1,
			max:   value,
		}
	} else {
		agg := a.metrics[key]
		agg.sum += value
		agg.count++
		if value > agg.max {
			agg.max = value
		}
	}
}

func (a *metricAggregator) emitAggregatedMetrics(ch chan<- prometheus.Metric) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, agg := range a.metrics {
		labels := []string{"nebula_cluster", "componentType"}
		labelValues := []string{key.cluster, key.componentType}

		if key.namespace != NonNamespace {
			labels = append(labels, "namespace")
			labelValues = append(labelValues, key.namespace)
		}

		// Emit based on metric type
		switch key.metricName {
		case "nebula_graphd_query_latency_us_p99_60":
			// Emit avg
			avg := agg.sum / float64(agg.count)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					"nebula_graphd_query_latency_us_p99_60_service_avg",
					"Service-level average of p99 query latency",
					labels,
					nil,
				),
				prometheus.GaugeValue,
				avg,
				labelValues...,
			)

			// Emit max
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					"nebula_graphd_query_latency_us_p99_60_service_max",
					"Service-level maximum of p99 query latency",
					labels,
					nil,
				),
				prometheus.GaugeValue,
				agg.max,
				labelValues...,
			)

		case "nebula_graphd_num_query_errors_sum_60", "nebula_graphd_num_queries_sum_60":
			// Emit sum
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					key.metricName+"_service_sum",
					"Service-level sum of "+key.metricName,
					labels,
					nil,
				),
				prometheus.GaugeValue,
				agg.sum,
				labelValues...,
			)
		}
	}
}
