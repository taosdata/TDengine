import { describe, expect, it } from 'vitest';
import source from './license.vue?raw';

describe('license.vue', () => {
  it('shows the cls config section only when cls is enabled', () => {
    expect(source).toContain('<template v-if="shouldShowClsInfo(currentClsConfig)">');
    expect(source).not.toContain(":disabled=\"currentClsConfig.clsEnabled === '1'\"");
  });

  it('builds cls info items through the shared helper with localized fallback text', () => {
    expect(source).toContain("buildClsInfoItems(currentClsConfig, t('topic.none'))");
  });

  it('keeps cls config labels on a single line', () => {
    expect(source).toContain(':label-style="clsLabelStyle"');
    expect(source).toContain("'white-space': 'nowrap'");
    expect(source).toContain('.license-descriptions .el-descriptions__label');
    expect(source).toContain('width: 140px !important;');
  });

  it('uses a shared fixed-width descriptions layout for cls and basic info', () => {
    expect(source).toContain('class="license-descriptions"');
    expect(source).toContain('.license-descriptions .el-descriptions__table');
    expect(source).toContain('table-layout: fixed;');
    expect(source).toContain(".license-descriptions .el-descriptions-item__cell");
    expect(source).toContain("width: 33.33%;");
  });

  it('waits for the shared show-variables settle delay only on the opted-in refresh path', () => {
    expect(source).toContain('SHOW_VARIABLES_SETTLE_DELAY_MS');
    expect(source).toContain('async function getData(waitForClsSettle = false)');
    expect(source).toContain('if (waitForClsSettle) {');
    expect(source).toContain('setTimeout(resolve, SHOW_VARIABLES_SETTLE_DELAY_MS)');
    expect(source).toContain('refresh(true);');
    expect(source).toContain('getData(waitForClsSettle);');
  });
});
