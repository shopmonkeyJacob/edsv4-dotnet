using Prometheus;

namespace EDS.Infrastructure.Metrics;

/// <summary>
/// Prometheus metrics for EDS, mirroring the Go metrics.go definitions.
/// </summary>
public static class EdsMetrics
{
    public static readonly Gauge PendingEvents = Prometheus.Metrics.CreateGauge(
        "eds_pending_events",
        "Current number of pending events buffered for processing.");

    public static readonly Counter TotalEvents = Prometheus.Metrics.CreateCounter(
        "eds_total_events",
        "Total number of events processed since startup.");

    public static readonly Histogram FlushDurationSeconds = Prometheus.Metrics.CreateHistogram(
        "eds_flush_duration_seconds",
        "Duration in seconds for the driver to flush data to the destination.",
        new HistogramConfiguration
        {
            Buckets = Histogram.ExponentialBuckets(0.001, 2, 14)
        });

    public static readonly Histogram FlushCount = Prometheus.Metrics.CreateHistogram(
        "eds_flush_count",
        "Number of events pending when flushed to the destination.",
        new HistogramConfiguration
        {
            Buckets = Histogram.ExponentialBuckets(1, 2, 14)
        });

    public static readonly Histogram ProcessingDurationSeconds = Prometheus.Metrics.CreateHistogram(
        "eds_processing_duration_seconds",
        "Duration in seconds to process a single event.",
        new HistogramConfiguration
        {
            Buckets = Histogram.ExponentialBuckets(0.0001, 2, 16)
        });
}
