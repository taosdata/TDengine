<template>
  <codemirror
    v-model="code"
    :placeholder="props.placeholder"
    class="w-full"
    :style="{ height: props.height }"
    :autofocus="true"
    :indent-with-tab="true"
    :disabled="props.disabled"
    :tab-size="2"
    :extensions="extensions"
    @ready="handleReady"
  />
</template>

<script lang="ts" setup>
import { Codemirror } from 'vue-codemirror';
import { sql } from '@codemirror/lang-sql';
import { lintGutter } from '@codemirror/lint';
import { search } from '@codemirror/search';
import { keymap } from '@codemirror/view';
import { defaultKeymap } from '@codemirror/commands';
import { autocompletion, CompletionContext } from '@codemirror/autocomplete';
import { TDengineSqlKeywrods } from 'constants1/tdengine';
import { basicSetup } from 'codemirror';

const props = withDefaults(
  defineProps<{
    modelValue: string;
    placeholder?: string;
    disabled?: boolean;
    height?: string;
    dbList?: Recordable[];
  }>(),
  {
    placeholder: 'Code goes here...',
    disabled: false,
    height: '400px',
    dbList: () => []
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
const keywords = TDengineSqlKeywrods.concat(TDengineSqlKeywrods.map(item => item.toLowerCase()));
const extensions = shallowRef<any[]>([]);
const emits = defineEmits(['update:modelValue', 'execute', 'ready']);
// Codemirror EditorView instance ref
const view = shallowRef();

const handleReady = (payload: Recordable) => {
  view.value = payload.view;
  emits('ready', payload);
};
setExtension();
function setExtension() {
  const shiftEnter = defaultKeymap.find(item => item.key == 'Enter');
  if (shiftEnter) {
    shiftEnter.shift = () => {
      emits('execute');
      return true;
    };
  }
  extensions.value = [
    basicSetup,
    keymap.of(defaultKeymap),
    sql({}),
    autocompletion({
      override: [myCompletions]
    }),
    lintGutter(),
    search()
  ];
}
function myCompletions(context: CompletionContext) {
  const word = context.matchBefore(/\w*/);
  if (!word) return null;
  if (word.from == word.to && !context.explicit) return null;
  return {
    from: word.from,
    options: keywords
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
      )
  };
}
</script>
<style scoped>
:deep(.cm-editor) {
  width: 100%;
}
</style>
