import { describe, expect, it } from 'vitest';
import source from './index.vue?raw';

describe('2_dataIn/index.vue', () => {
  it('blocks the gate when the initial xnode prefetch fails', () => {
    expect(source).toContain('} catch {');
    expect(source).toContain('xnodesExist.value = false;');
  });

  it('refreshes xnode availability again when the cached view is re-activated', () => {
    expect(source).toContain('async function refreshXnodesExist() {');
    expect(source).toContain('onMounted(refreshXnodesExist);');
    expect(source).toContain('onActivated(refreshXnodesExist);');
  });

  it('suppresses passive SQL alerts during xnode prefetch and gate checks', () => {
    expect(source).toContain("sendSQLReq('show xnodes;', false, false)");
  });
});
