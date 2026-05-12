const METRIC_IN_ORDER = [
  'start_time',
  'execute_time',
  'created_stables',
  'created_tables',
  'received_batches',
  'processed_batches',
  'received_messages',
  'processed_messages',
  'processed_rows',
  'written_rows',
  'written_raw_blocks',
  'written_points',
  'rows_per_second',
  'points_per_second'
];

const HIDDEN_METRICS_BY_TAB: Record<string, Set<string>> = {
  current: new Set(['job_id', 'progress_snapshot'])
};

type MetricRow = {
  name: string;
  value: unknown;
};

type MetricGroup = {
  name: string;
  metrics: MetricRow[];
};

type MetricPayload = Record<string, Record<string, unknown>>;

function shouldHideMetric(tabName: string, metricName: string) {
  return HIDDEN_METRICS_BY_TAB[tabName]?.has(metricName) ?? false;
}

export function buildMetricsArray(metricsData: MetricPayload): MetricGroup[] {
  return Object.keys(metricsData).map(name => {
    const source = metricsData[name];
    const metrics: MetricRow[] = [];

    for (const metricName of METRIC_IN_ORDER) {
      const value = source[metricName];
      if (value !== undefined) {
        metrics.push({
          name: metricName,
          value
        });
      }
    }

    for (const metricName in source) {
      if (METRIC_IN_ORDER.includes(metricName) || shouldHideMetric(name, metricName)) {
        continue;
      }

      metrics.push({
        name: metricName,
        value: source[metricName]
      });
    }

    return { name, metrics };
  });
}
