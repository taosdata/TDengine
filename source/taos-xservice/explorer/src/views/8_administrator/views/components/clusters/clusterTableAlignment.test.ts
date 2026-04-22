import { describe, expect, it } from 'vitest';
import dnodesSource from './dnodes.vue?raw';
import mnodesSource from './mnodes.vue?raw';
import qnodesSource from './qnodes.vue?raw';
import xnodesSource from './xnodes.vue?raw';
import anodesSource from './anodes.vue?raw';

describe('cluster node table alignment', () => {
  it('uses shared widths instead of hardcoded endpoint/create-time/action widths', () => {
    expect(dnodesSource).toContain("import { CLUSTER_TABLE_WIDTHS } from './clusterTableColumns';");
    for (const slot of ['endpoint', 'extensionA', 'extensionB', 'status', 'createTime', 'action']) {
      expect(dnodesSource).toContain(`:min-width="CLUSTER_TABLE_WIDTHS.${slot}"`);
      expect(dnodesSource).not.toContain(`:width="CLUSTER_TABLE_WIDTHS.${slot}"`);
    }

    expect(mnodesSource).toContain("import { CLUSTER_TABLE_WIDTHS } from './clusterTableColumns';");
    for (const slot of ['endpoint', 'extensionA', 'extensionB', 'status', 'createTime', 'action']) {
      expect(mnodesSource).toContain(`:min-width="CLUSTER_TABLE_WIDTHS.${slot}"`);
      expect(mnodesSource).not.toContain(`:width="CLUSTER_TABLE_WIDTHS.${slot}"`);
    }

    for (const source of [dnodesSource, mnodesSource]) {
      expect(source).not.toContain('width="400"');
      expect(source).not.toContain('width="240"');
      expect(source).not.toContain('width="65"');
      expect(source).not.toContain(':width="360"');
      expect(source).not.toContain(':width="160"');
      expect(source).not.toContain(':width="240"');
      expect(source).not.toContain(':width="72"');
    }
  });

  it('keeps sparse tables on the same shared trailing-column rails', () => {
    for (const source of [qnodesSource, xnodesSource, anodesSource]) {
      expect(source).not.toContain(':width="360"');
      expect(source).not.toContain(':width="180"');
      expect(source).not.toContain(':width="160"');
      expect(source).not.toContain(':width="240"');
      expect(source).not.toContain(':width="72"');
    }

    for (const source of [qnodesSource, xnodesSource, anodesSource]) {
      expect(source).toContain("import { CLUSTER_TABLE_WIDTHS } from './clusterTableColumns';");
      for (const slot of ['endpoint', 'extensionA', 'extensionB', 'status', 'createTime', 'action']) {
        expect(source).toContain(`:min-width="CLUSTER_TABLE_WIDTHS.${slot}"`);
        expect(source).not.toContain(`:width="CLUSTER_TABLE_WIDTHS.${slot}"`);
      }
    }

    expect(qnodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionA" />');
    expect(qnodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionB" />');
    expect(qnodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.status" />');
    expect(qnodesSource).not.toContain(':label="$t(\'taoscluster.status\')"');
    expect(qnodesSource).not.toContain('prop="status"');

    expect(xnodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionA" />');
    expect(xnodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionB" />');
    expect(xnodesSource).toContain(':min-width="CLUSTER_TABLE_WIDTHS.status"');

    expect(anodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionA" />');
    expect(anodesSource).toContain('<el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionB" />');
    expect(anodesSource).toContain(':min-width="CLUSTER_TABLE_WIDTHS.status"');
  });
});
