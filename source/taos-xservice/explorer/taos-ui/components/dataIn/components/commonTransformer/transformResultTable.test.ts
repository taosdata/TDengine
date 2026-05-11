import { mount } from '@vue/test-utils';
import { nextTick } from 'vue';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ResultTable from './transformResultTable.vue';
import { resetTransformerPreviewState, showEmptyTransformerPreview, transformerState } from './util';

vi.mock('locales', () => ({
  t: (key: string) => key
}));

describe('TransformResultTable empty preview rendering', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'ResizeObserver',
      class {
        observe() {}
        disconnect() {}
      }
    );
  });

  afterEach(() => {
    resetTransformerPreviewState();
    vi.unstubAllGlobals();
  });

  it('renders an empty preview table after the user closes a previous preview and the next response is empty', async () => {
    transformerState.transformResultTable = [{ a: '1' }];
    transformerState.resultTbTitle = 'parseResTb';
    transformerState.transResultName = 'dataIn.transformer.identified';
    transformerState.showResultTb = false;

    showEmptyTransformerPreview('parseResTb', 'dataIn.transformer.identified');

    const wrapper = mount(ResultTable, {
      props: {
        isEditable: false,
        currentDataSource: 'kafka'
      },
      global: {
        stubs: {
          Close: true,
          FullScreen: true,
          'el-drawer': {
            template: '<div><slot /></div>'
          },
          'el-empty': {
            template: '<div data-test="preview-empty"></div>'
          },
          'el-icon': {
            template: '<span><slot /></span>'
          },
          'el-pagination': true,
          'el-table': {
            template: '<div data-test="preview-table"><slot /></div>'
          },
          'el-table-column': {
            template: '<div data-test="preview-column"><slot /></div>'
          },
          'el-tooltip': {
            template: '<span><slot /><slot name="content" /></span>'
          }
        }
      }
    });
    await nextTick();

    expect(wrapper.find('[data-test="preview-table"]').exists()).toBe(true);
    expect(wrapper.find('[data-test="preview-empty"]').exists()).toBe(false);
  });
});
