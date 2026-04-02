import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { reactive, ref } from 'vue';

describe('resetTransformerPreviewState', () => {
  beforeEach(() => {
    vi.stubGlobal('reactive', reactive);
    vi.stubGlobal('ref', ref);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('clears visible preview result state', async () => {
    const { resetTransformerPreviewState, transformerState } = await import('./util');

    transformerState.transformResultTable = [{ a: 1 }];
    transformerState.transResultName = 'identified';
    transformerState.showResultTb = true;
    transformerState.resultTbTitle = 'parseResTb';
    transformerState.activeColumns = ['a'];
    transformerState.resultCurrentPage = 3;

    resetTransformerPreviewState();

    expect(transformerState.transformResultTable).toEqual([]);
    expect(transformerState.transResultName).toBe('');
    expect(transformerState.showResultTb).toBe(false);
    expect(transformerState.resultTbTitle).toBe('');
    expect(transformerState.activeColumns).toEqual([]);
    expect(transformerState.resultCurrentPage).toBe(1);
  });
});
