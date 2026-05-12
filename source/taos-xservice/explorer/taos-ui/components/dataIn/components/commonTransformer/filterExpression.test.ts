/* eslint-disable vue/one-component-per-file */
import { defineComponent } from 'vue';
import { mount } from '@vue/test-utils';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import FilterExpression from './filterExpression.vue';
import { dataInPropsKey } from '../../model/useDataIn';
import { transformerState } from './util';

const ElInputStub = defineComponent({
  name: 'ElInput',
  props: {
    modelValue: {
      type: String,
      default: ''
    },
    size: {
      type: String,
      default: ''
    }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit }) {
    function onInput(event: Event) {
      const value = (event.target as HTMLInputElement).value;
      emit('update:modelValue', value);
      emit('change', value);
    }

    return { props, onInput };
  },
  template: '<input v-bind="$attrs" :value="props.modelValue" @input="onInput" />'
});

const ElFormStub = defineComponent({
  name: 'ElForm',
  template: '<form><slot /></form>'
});

const ElFormItemStub = defineComponent({
  name: 'ElFormItem',
  template: '<div><slot /></div>'
});

function mountFilterExpression(expression: string | { expr: string; null_if_error?: boolean }) {
  return mount(FilterExpression, {
    props: {
      itemData: {
        key: 'filter-1',
        expression
      },
      payload: '',
      identifiedColumns: [],
      msgForm: {
        msgbody: '{"value":1}'
      },
      datasourceType: 'kafka'
    },
    global: {
      provide: {
        [dataInPropsKey]: {
          isCommunity: false,
          transform: {
            api: {
              getParser: vi.fn().mockResolvedValue([{ fields: [], columns: [] }])
            }
          }
        },
        generateInput: () => [{ value: '{"value":1}' }]
      },
      stubs: {
        Icon: true,
        ElInput: ElInputStub,
        ElForm: ElFormStub,
        ElFormItem: ElFormItemStub
      }
    }
  });
}

describe('FilterExpression', () => {
  beforeEach(() => {
    transformerState.transformerFilterParseData = null;
  });

  it('keeps legacy string filters readable in the input', async () => {
    const wrapper = mountFilterExpression('DeviceNo > 1');

    await wrapper.vm.$nextTick();

    expect((wrapper.get('input').element as HTMLInputElement).value).toBe('DeviceNo > 1');
  });

  it('hydrates structured Kafka rule filters back to plain text', async () => {
    const wrapper = mountFilterExpression({ expr: 'DeviceNo > 1' });

    await wrapper.vm.$nextTick();

    expect((wrapper.get('input').element as HTMLInputElement).value).toBe('DeviceNo > 1');
  });

  it('preserves structured filter options when submitting a hydrated filter', async () => {
    const wrapper = mountFilterExpression({ expr: 'DeviceNo > 1', null_if_error: false });

    await wrapper.vm.$nextTick();
    (wrapper.vm as unknown as { submitFilter: () => void }).submitFilter();

    expect(transformerState.transformerFilterParseData).toEqual({
      filter: {
        expr: 'DeviceNo > 1',
        null_if_error: false
      }
    });
  });

  it('stores the trimmed filter expression after submit', async () => {
    const wrapper = mountFilterExpression({ expr: 'DeviceNo > 1', null_if_error: false });

    await wrapper.vm.$nextTick();
    await wrapper.get('input').setValue('  DeviceNo > 2  ');
    (wrapper.vm as unknown as { submitFilter: () => void }).submitFilter();

    expect(transformerState.transformerFilterParseData).toEqual({
      filter: {
        expr: 'DeviceNo > 2',
        null_if_error: false
      }
    });
  });
});
