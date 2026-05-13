/* eslint-disable vue/one-component-per-file -- inline stubs keep this focused test self-contained. */
import { mount } from '@vue/test-utils';
import { defineComponent, h, nextTick } from 'vue';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import TaskImport from './task-import.vue';
import source from './task-import.vue?raw';

const dataInProps = {
  uploadFileUrl: '/upload',
  isCommunity: false,
  xnodesExist: null as boolean | null,
  missingXnodeCallback: vi.fn(),
  ensureXnodeThen: vi.fn(),
  dataSource: {
    api: {
      getDatabase: vi.fn().mockResolvedValue([])
    }
  },
  task: {
    api: {
      importTask: vi.fn()
    }
  }
};

vi.mock('locales', () => ({
  t: (key: string) => key
}));

vi.mock('config', () => ({
  instance: {
    gatewayUrl: 'http://localhost:6041',
    user: 'root',
    password: 'taosdata'
  }
}));

vi.mock('../../dataIn/model/useDataIn', () => ({
  getDataInProps: () => dataInProps
}));

vi.mock('../../dataIn/model/util', () => ({
  agentList: []
}));

const ElUpload = defineComponent({
  name: 'ElUpload',
  setup(_props, { slots }) {
    return () => h('div', slots.default?.());
  }
});

const ElDialog = defineComponent({
  name: 'ElDialog',
  props: {
    modelValue: {
      type: Boolean,
      default: false
    }
  },
  setup(props, { slots }) {
    return () => (props.modelValue ? h('div', slots.default?.()) : null);
  }
});

const ElButton = defineComponent({
  name: 'ElButton',
  props: {
    disabled: {
      type: Boolean,
      default: false
    }
  },
  emits: ['click'],
  setup(props, { emit, slots }) {
    return () =>
      h(
        'button',
        {
          disabled: props.disabled,
          onClick: () => emit('click')
        },
        slots.default?.()
      );
  }
});

const passthrough = defineComponent({
  setup(_props, { slots }) {
    return () => h('div', slots.default?.());
  }
});

function mountTaskImport() {
  return mount(TaskImport, {
    global: {
      stubs: {
        ElUpload,
        ElButton,
        ElDialog,
        ElTable: passthrough,
        ElTableColumn: passthrough,
        ElSelect: passthrough,
        ElOption: passthrough
      }
    }
  });
}

describe('task-import.vue', () => {
  beforeEach(() => {
    dataInProps.xnodesExist = null;
    dataInProps.missingXnodeCallback?.mockReset();
    dataInProps.missingXnodeCallback = vi.fn();
    dataInProps.ensureXnodeThen.mockReset();
    dataInProps.dataSource.api.getDatabase.mockClear();
  });

  it('disables the import trigger until the xnode precheck finishes', async () => {
    const wrapper = mountTaskImport();

    await nextTick();

    expect(wrapper.get('button').attributes('disabled')).toBeDefined();
  }, 15000);

  it('shows the missing-xnode guidance instead of opening import while xnodes are absent', async () => {
    dataInProps.xnodesExist = false;
    const wrapper = mountTaskImport();

    await wrapper.get('button').trigger('click');

    expect(dataInProps.missingXnodeCallback).toHaveBeenCalledTimes(1);
    expect(dataInProps.ensureXnodeThen).not.toHaveBeenCalled();
  }, 15000);

  it('does not fall back to async file-picker opening when xnodes are already known missing', async () => {
    dataInProps.xnodesExist = false;
    dataInProps.missingXnodeCallback = undefined as any;
    const wrapper = mountTaskImport();

    await wrapper.get('button').trigger('click');

    expect(dataInProps.ensureXnodeThen).not.toHaveBeenCalled();
  }, 15000);

  it('documents the synchronous picker path only for the xnodesExist === true case', () => {
    expect(source).toContain('// xnodesExist === true: open file picker synchronously');
    expect(source).not.toContain('true or null');
  });
});
