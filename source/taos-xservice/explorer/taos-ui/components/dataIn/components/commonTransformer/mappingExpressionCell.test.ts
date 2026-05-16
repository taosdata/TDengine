/* eslint-disable vue/one-component-per-file */
import { defineComponent } from 'vue';
import { mount } from '@vue/test-utils';
import { describe, expect, it, vi } from 'vitest';
import MappingExpressionCell from './mappingExpressionCell.vue';
import type { TableRow } from './type';

const ElSelectStub = defineComponent({
  name: 'ElSelect',
  props: {
    modelValue: {
      type: [String, Number, Array],
      default: ''
    },
    multiple: Boolean
  },
  emits: ['update:modelValue', 'change', 'clear'],
  setup(props, { emit }) {
    function onChange(event: Event) {
      const target = event.target as HTMLSelectElement;
      emit('update:modelValue', target.value);
      emit('change', target.value);
    }

    return { props, onChange };
  },
  template:
    '<select v-bind="$attrs" :value="props.modelValue" :multiple="props.multiple" @change="onChange"><slot /></select>'
});

const ElOptionStub = defineComponent({
  name: 'ElOption',
  props: {
    label: {
      type: String,
      default: ''
    },
    value: {
      type: String,
      default: ''
    }
  },
  template: '<option :value="value">{{ label }}</option>'
});

const ElInputStub = defineComponent({
  name: 'ElInput',
  props: {
    modelValue: {
      type: [String, Number, Array],
      default: ''
    },
    size: {
      type: String,
      default: ''
    }
  },
  emits: ['update:modelValue', 'change', 'blur'],
  setup(props, { emit }) {
    function onInput(event: Event) {
      emit('update:modelValue', (event.target as HTMLInputElement).value);
    }

    function onChange(event: Event) {
      emit('change', (event.target as HTMLInputElement).value);
    }

    function onBlur(event: Event) {
      emit('blur', event);
    }

    return { props, onInput, onChange, onBlur };
  },
  template: '<input v-bind="$attrs" :value="props.modelValue" @input="onInput" @change="onChange" @blur="onBlur" />'
});

const ElTooltipStub = defineComponent({
  name: 'ElTooltip',
  template: '<div class="el-tooltip-stub"><slot /></div>'
});

const ElIconStub = defineComponent({
  name: 'ElIcon',
  template: '<span class="el-icon-stub"><slot /></span>'
});

function createRow(overrides: Partial<TableRow> = {}): TableRow {
  return {
    Name: 'ts',
    Type: 'TIMESTAMP',
    exprname: 'mapping',
    maptype: ['string', 'TIMESTAMP'],
    Expression: '',
    PrimaryKey: true,
    ...overrides
  };
}

function mountCell(row: TableRow) {
  return mount(MappingExpressionCell, {
    props: {
      row,
      mappingTypes: ['mapping', 'value', 'generator', 'join', 'format', 'sum', 'expr'],
      mappingcolumns: [{ label: 'value', value: 'value' }],
      exprformat: '${ts}',
      exprexpression: 'now()',
      onDefaultValueInput: vi.fn()
    },
    global: {
      stubs: {
        Icon: true,
        ElSelect: ElSelectStub,
        ElOption: ElOptionStub,
        ElInput: ElInputStub,
        ElTooltip: ElTooltipStub,
        ElIcon: ElIconStub
      }
    }
  });
}

describe('MappingExpressionCell', () => {
  it('applies the same generator state for Kafka and non-Kafka rows', async () => {
    const kafkaRow = createRow();
    const legacyRow = createRow();

    const kafka = mountCell(kafkaRow);
    const legacy = mountCell(legacyRow);

    await kafka.get('.mapping-rule-select').setValue('generator');
    await legacy.get('.mapping-rule-select').setValue('generator');

    expect(kafkaRow.Expression).toBe('now');
    expect(legacyRow.Expression).toBe('now');
    expect(kafka.findComponent({ name: 'ElTooltip' }).exists()).toBe(true);
    expect(legacy.findComponent({ name: 'ElTooltip' }).exists()).toBe(true);
    expect(kafka.get('input').attributes('disabled')).toBeDefined();
    expect(legacy.get('input').attributes('disabled')).toBeDefined();
  });

  it('trims SubTableName expression on blur', async () => {
    const row = createRow({
      Name: 'SubTableName',
      Expression: '  topic_name  '
    });

    const wrapper = mountCell(row);
    await wrapper.get('input').trigger('blur');

    expect(row.Expression).toBe('topic_name');
    expect(wrapper.emitted('changed')).toHaveLength(1);
  });

  it('does not trim SubTableName expression when it is not a string', async () => {
    const expression = ['topic_a', 'topic_b'];
    const row = createRow({
      Name: 'SubTableName',
      Expression: expression
    });

    const wrapper = mountCell(row);
    await wrapper.get('input').trigger('blur');

    expect(row.Expression).toBe(expression);
    expect(wrapper.emitted('changed')).toHaveLength(1);
  });

  it('emits changed when the expression input changes', async () => {
    const row = createRow({
      exprname: 'expr',
      Expression: 'now()'
    });

    const wrapper = mountCell(row);
    await wrapper.get('input').setValue('time()');

    expect(wrapper.emitted('changed')).toHaveLength(1);
  });

  it('clears mapping defaults before switching a row to generator', async () => {
    const row = createRow({
      exprname: 'mapping',
      Expression: 'source_ts',
      default: '123',
      defaultValueError: 'bad value'
    });

    const wrapper = mountCell(row);
    await wrapper.get('.mapping-rule-select').setValue('generator');

    expect(row.Expression).toBe('now');
    expect(row.default).toBe('');
    expect(row.defaultValueError).toBe('');
  });
});
