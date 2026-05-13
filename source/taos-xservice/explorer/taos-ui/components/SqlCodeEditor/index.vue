<template>
  <codemirror
    v-model="code"
    :placeholder="placeholder"
    class="w-full"
    :style="{ height: props.height, minHeight: props.minHeight, lineHeight: 1.8 }"
    :autofocus="true"
    :indent-with-tab="true"
    :disabled="props.disabled"
    :tab-size="2"
    :extensions="extensions"
    @ready="handleReady"
  />
</template>

<script setup lang="ts">
import { Codemirror } from 'vue-codemirror';
import { sql } from '@codemirror/lang-sql';
import { search } from '@codemirror/search';
import { keymap, EditorView, ViewPlugin } from '@codemirror/view';
import { defaultKeymap } from '@codemirror/commands';
import { autocompletion, CompletionContext } from '@codemirror/autocomplete';
import { TDengineSqlKeywrods } from 'constants1/tdengine';
import { basicSetup } from 'codemirror';
import { t } from 'locales';
import { isEqual } from 'lodash-es';
const props = withDefaults(
  defineProps<{
    modelValue: string;
    placeholder?: string;
    disabled?: boolean;
    height?: string;
    minHeight?: string;
    dbList?: Recordable[];
    placeholders?: ViewPlugin<any>;
    otherCompletions?: any[];
  }>(),
  {
    placeholder: '',
    disabled: false,
    height: '400px',
    minHeight: '400px',
    dbList: () => [],
    placeholders: undefined,
    otherCompletions: undefined
  }
);

const code = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    emits('update:modelValue', value);
  }
});

const placeholder = computed(() => {
  return props.placeholder || t('explorer.sqlGoesHere');
});
const keywords = TDengineSqlKeywrods.concat(TDengineSqlKeywrods.map(item => item.toLowerCase()));
const extensions = shallowRef<any[]>([]);
const emits = defineEmits(['update:modelValue', 'execute', 'ready', 'format']);
// Codemirror EditorView instance ref
const view = shallowRef();

const handleReady = (payload: Recordable) => {
  view.value = payload.view;
  emits('ready', payload);
};
const formatKey = {
  key: 'Ctrl-Shift-f',
  mac: 'Cmd-Shift-f', // Mac 系统使用 Cmd
  run: () => {
    // 发出格式化事件
    emits('format');
    return true;
  }
};
// 监听 placeholders 变化
watch(
  () => props.placeholders,
  (newVal, oldVal) => {
    if (newVal && !isEqual(newVal, oldVal)) {
      console.log('placeholders changed:', newVal); // 调试用
      setExtension();
    }
  }
);

function setExtension() {
  const shiftEnter = defaultKeymap.find(item => item.key == 'Enter');
  if (shiftEnter) {
    shiftEnter.shift = () => {
      emits('execute');
      return true;
    };
  }
  const extensionsValue = [
    basicSetup,
    keymap.of([...defaultKeymap, formatKey]),
    sql({}),
    autocompletion({
      override: [myCompletions]
    })
  ];
  if (props.placeholders) {
    extensionsValue.push(props.placeholders);
  }
  extensionsValue.push(search(), EditorView.lineWrapping); // 添加自动换行配置);
  extensions.value = extensionsValue;
}
function myCompletions(context: CompletionContext) {
  const word = context.matchBefore(/[\w\u4e00-\u9fa5]*/);
  if (!word) return null;
  if (word.from == word.to && !context.explicit) return null;
  const optionsValue = keywords
    .filter(option => option.startsWith(word.text))
    .map(option => {
      return {
        label: option,
        type: 'keyword'
      };
    })
    .concat(
      props.dbList.map(item => {
        return {
          label: item.name,
          type: 'namespace',
          info: 'Database'
        };
      })
    );
  if (props.otherCompletions) {
    optionsValue.push(...props.otherCompletions);
  }
  return {
    from: word.from,
    validFor: /^[\w\u4e00-\u9fa5]*$/,
    options: optionsValue
  };
}
onMounted(() => {
  setExtension();
});
</script>

<style scoped>
:deep(.cm-editor) {
  width: 100%;
}
:deep(.cm-scroller) {
  padding-top: 5px;
}
</style>
