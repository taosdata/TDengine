import { describe, expect, it } from 'vitest';
import source from './xnodes.vue?raw';

describe('xnodes.vue', () => {
  it('keeps the status column aligned with the other cluster blocks', () => {
    expect(source).toContain(':min-width="CLUSTER_TABLE_WIDTHS.endpoint"');
    expect(source).toContain('prop="endpoint"');
    expect(source).toContain(':min-width="CLUSTER_TABLE_WIDTHS.extensionA"');
    expect(source).toContain(':min-width="CLUSTER_TABLE_WIDTHS.extensionB"');
    expect(source).toContain(':min-width="CLUSTER_TABLE_WIDTHS.status"');
    expect(source).toContain('prop="status"');
  });

  it('paginates xnode rows before rendering the table', () => {
    expect(source).toContain('const pagedXnodesList = computed(() =>');
    expect(source).toContain('<el-table :data="pagedXnodesList" size="small">');
  });

  it('removes the unused page-change handler wiring', () => {
    expect(source).not.toContain('@current-change="handlePageChange"');
    expect(source).not.toContain('function handlePageChange()');
  });

  it('handles delete-dialog cancellation explicitly', () => {
    expect(source).toContain('}).catch(() => {');
  });
});
