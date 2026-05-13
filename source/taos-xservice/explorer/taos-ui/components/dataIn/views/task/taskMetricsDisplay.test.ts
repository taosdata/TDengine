import { describe, expect, it } from 'vitest';

import { buildMetricsArray } from './taskMetricsDisplay';

describe('taskMetricsDisplay', () => {
  it('hides internal metrics only from the current run tab', () => {
    const metricsArray = buildMetricsArray({
      current: {
        start_time: 1,
        job_id: -1,
        progress_snapshot: [{ topic: 'log2', vgroup: 2, offset: 10, latest: 20 }],
        commits: 3
      },
      total: {
        total_execute_time: 20,
        total_consume_cost_ms: 30
      }
    });

    expect(metricsArray).toEqual([
      {
        name: 'current',
        metrics: [
          { name: 'start_time', value: 1 },
          { name: 'commits', value: 3 }
        ]
      },
      {
        name: 'total',
        metrics: [
          { name: 'total_execute_time', value: 20 },
          { name: 'total_consume_cost_ms', value: 30 }
        ]
      }
    ]);
  });
});
