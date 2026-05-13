import { describe, expect, it } from 'vitest';
import { CLUSTER_TABLE_WIDTHS } from './clusterTableColumns';

describe('clusterTableColumns', () => {
  it('defines the shared cluster table slot widths', () => {
    expect(CLUSTER_TABLE_WIDTHS).toEqual({
      endpoint: 180,
      extensionA: 180,
      extensionB: 180,
      status: 180,
      createTime: 180,
      action: 180
    });
  });
});
