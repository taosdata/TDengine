/* eslint-disable vue/one-component-per-file -- inline stubs keep this focused test self-contained. */
import { flushPromises, mount } from '@vue/test-utils';
import { h } from 'vue';
import { createI18n } from 'vue-i18n';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const elMessageError = vi.fn();

const i18n = createI18n({
  legacy: false,
  locale: 'en',
  messages: {
    en: {
        taoscluster: {
          addxnodes: 'Add XNodes',
          authUserPassTab: 'User / Password',
          authTokenTab: 'Token',
          endpoint: 'End Point',
          user: 'User',
          password: 'Password',
          token: 'Token',
          endpointRequired: 'Please input the endpoint.',
          userPassRequired: 'User and password must be filled in together.',
          xnodeAuthModeExclusive: 'Use either token or user/password.',
          invalidUser: 'User contains unsupported characters.',
          createXnodeFailed: 'Failed to create XNode.'
        },
      cancel: 'Cancel',
      confirm: 'Confirm'
    }
  }
});

vi.mock('element-plus', async () => {
  const { defineComponent, inject, provide } = await import('vue');
  const tabsKey = Symbol('tabsKey');

  const ElDialog = defineComponent({
    name: 'ElDialog',
    props: {
      modelValue: {
        type: Boolean,
        required: true
      }
    },
    setup(props, { slots }) {
      return () => (props.modelValue ? h('div', slots.default?.()) : null);
    }
  });

  const ElForm = defineComponent({
    name: 'ElForm',
    setup(_props, { slots, expose }) {
      expose({
        validate: vi.fn().mockResolvedValue(true),
        clearValidate: vi.fn()
      });
      return (_ctx: Record<string, unknown>, attrs: Record<string, unknown>) => h('form', attrs, slots.default?.());
    }
  });

  const ElFormItem = defineComponent({
    name: 'ElFormItem',
    setup(_props, { slots }) {
      return () => h('div', slots.default?.());
    }
  });

  const ElInput = defineComponent({
    name: 'ElInput',
    props: {
      autocomplete: {
        type: String,
        default: undefined
      },
      modelValue: {
        type: String,
        default: ''
      }
    },
    emits: ['update:modelValue', 'keyup.enter'],
    setup(props, { attrs, emit }) {
      return () =>
        h('input', {
          ...attrs,
          autocomplete: props.autocomplete,
          value: props.modelValue,
          onInput: (event: Event) => emit('update:modelValue', (event.target as HTMLInputElement).value)
        });
    }
  });

  const ElButton = defineComponent({
    name: 'ElButton',
    emits: ['click'],
    setup(_props, { emit, slots }) {
      return () =>
        h(
          'button',
          {
            type: 'button',
            onClick: () => emit('click')
          },
          slots.default?.()
        );
    }
  });

  const ElRow = defineComponent({
    name: 'ElRow',
    setup(_props, { slots }) {
      return () => h('div', slots.default?.());
    }
  });

  const ElCol = defineComponent({
    name: 'ElCol',
    setup(_props, { slots }) {
      return () => h('div', slots.default?.());
    }
  });

  const ElTabs = defineComponent({
    name: 'ElTabs',
    props: {
      modelValue: {
        type: String,
        required: true
      }
    },
    emits: ['update:modelValue'],
    setup(props, { slots, emit }) {
      provide(tabsKey, () => props.modelValue);
      return () =>
        h('div', { 'data-auth-mode': props.modelValue }, [
          h(
            'button',
            {
              type: 'button',
              'data-tab': 'credentials',
              onClick: () => emit('update:modelValue', 'credentials')
            },
            'credentials'
          ),
          h(
            'button',
            {
              type: 'button',
              'data-tab': 'token',
              onClick: () => emit('update:modelValue', 'token')
            },
            'token'
          ),
          slots.default?.()
        ]);
    }
  });

  const ElTabPane = defineComponent({
    name: 'ElTabPane',
    props: {
      name: {
        type: String,
        required: true
      }
    },
    setup(props, { slots, attrs }) {
      const getActiveTab = inject<() => string>(tabsKey, () => 'credentials');
      return () => (getActiveTab() === props.name ? h('div', { ...attrs, 'data-pane': props.name }, slots.default?.()) : null);
    }
  });

  return {
    ElDialog,
    ElForm,
    ElFormItem,
    ElInput,
    ElButton,
    ElRow,
    ElCol,
    ElTabs,
    ElTabPane,
    ElMessage: {
      error: elMessageError
    }
  };
});

beforeEach(() => {
  elMessageError.mockReset();
});

describe('AddXnodeDialog', () => {
  it('defaults to the username/password tab', async () => {
    const { default: AddXnodeDialog } = await import('./AddXnodeDialog.vue');
    const wrapper = mount(AddXnodeDialog, {
      props: {
        modelValue: true,
        sendSql: vi.fn()
      },
      global: {
        plugins: [i18n]
      }
    });

    expect(wrapper.find('[data-auth-mode="credentials"]').exists()).toBe(true);
    expect(wrapper.find('[data-pane="credentials"]').exists()).toBe(true);
    expect(wrapper.find('[data-pane="token"]').exists()).toBe(false);
  });

  it('submits the escaped create xnode SQL and emits success', async () => {
    const sendSql = vi.fn().mockResolvedValue({ code: 0 });
    const { default: AddXnodeDialog } = await import('./AddXnodeDialog.vue');
    const wrapper = mount(AddXnodeDialog, {
      props: {
        modelValue: true,
        sendSql
      },
      global: {
        plugins: [i18n]
      }
    });

    const inputs = wrapper.findAll('input');
    await inputs[0].setValue("x'1:6050");
    await inputs[1].setValue('__xnode__');
    await inputs[2].setValue("Ab\\'123456");

    await wrapper.findAll('button').at(-1)?.trigger('click');
    await flushPromises();

    expect(sendSql).toHaveBeenCalledWith("create xnode 'x''1:6050' user __xnode__ pass 'Ab\\\\''123456';");
    expect(wrapper.emitted('success')).toHaveLength(1);
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([false]);
  });

  it('keeps the dialog open when xnode creation does not succeed', async () => {
    const sendSql = vi.fn().mockRejectedValue(new Error('create xnode failed'));
    const { default: AddXnodeDialog } = await import('./AddXnodeDialog.vue');
    const wrapper = mount(AddXnodeDialog, {
      props: {
        modelValue: true,
        sendSql
      },
      global: {
        plugins: [i18n]
      }
    });

    const inputs = wrapper.findAll('input');
    await inputs[0].setValue('x1:6050');
    await wrapper.findAll('button').at(-1)?.trigger('click');
    await flushPromises();

    expect(sendSql).toHaveBeenCalledWith("create xnode 'x1:6050';");
    expect(elMessageError).toHaveBeenCalledWith('create xnode failed');
    expect(wrapper.emitted('success')).toBeUndefined();
    expect(wrapper.emitted('update:modelValue')).toBeUndefined();
  });

  it('submits token auth SQL when the user provides a token', async () => {
    const sendSql = vi.fn().mockResolvedValue({ code: 0 });
    const { default: AddXnodeDialog } = await import('./AddXnodeDialog.vue');
    const wrapper = mount(AddXnodeDialog, {
      props: {
        modelValue: true,
        sendSql
      },
      global: {
        plugins: [i18n]
      }
    });

    await wrapper.find('[data-tab="token"]').trigger('click');

    expect(wrapper.find('[data-auth-mode="token"]').exists()).toBe(true);
    expect(wrapper.find('[data-pane="credentials"]').exists()).toBe(false);
    expect(wrapper.find('[data-pane="token"]').exists()).toBe(true);

    const inputs = wrapper.findAll('input');
    await inputs[0].setValue('192.168.1.10:6043');
    await inputs[1].setValue("token\\'123");

    await wrapper.findAll('button').at(-1)?.trigger('click');
    await flushPromises();

    expect(sendSql).toHaveBeenCalledWith("create xnode '192.168.1.10:6043' token 'token\\\\''123';");
    expect(wrapper.emitted('success')).toHaveLength(1);
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([false]);
  });

  it('shows the backend error description when xnode creation returns a non-zero code', async () => {
    const sendSql = vi.fn().mockResolvedValue({ code: 1, desc: 'xnode already exists' });
    const { default: AddXnodeDialog } = await import('./AddXnodeDialog.vue');
    const wrapper = mount(AddXnodeDialog, {
      props: {
        modelValue: true,
        sendSql
      },
      global: {
        plugins: [i18n]
      }
    });

    const inputs = wrapper.findAll('input');
    await inputs[0].setValue('x1:6050');
    await wrapper.findAll('button').at(-1)?.trigger('click');
    await flushPromises();

    expect(sendSql).toHaveBeenCalledWith("create xnode 'x1:6050';");
    expect(elMessageError).toHaveBeenCalledWith('xnode already exists');
    expect(wrapper.emitted('success')).toBeUndefined();
    expect(wrapper.emitted('update:modelValue')).toBeUndefined();
  });

  it('disables browser autofill for the xnode form inputs', async () => {
    const { default: AddXnodeDialog } = await import('./AddXnodeDialog.vue');
    const wrapper = mount(AddXnodeDialog, {
      props: {
        modelValue: true,
        sendSql: vi.fn()
      },
      global: {
        plugins: [i18n]
      }
    });

    expect(wrapper.find('form').attributes('autocomplete')).toBe('off');

    const inputs = wrapper.findAll('input');
    expect(inputs[0].attributes('autocomplete')).toBe('off');
    expect(inputs[1].attributes('autocomplete')).toBe('off');
    expect(inputs[2].attributes('autocomplete')).toBe('new-password');
  });
});
