<template>
  <div class="rich-text-editor-wrapper">
    <Toolbar class="rich-text-editor-toolbar" :editor="editorRef" :default-config="toolbarConfig" :mode="props.mode" />
    <Editor
      v-model="currentValue"
      class="rich-text-editor-content"
      :style="{ height: props.height }"
      :default-config="props.editorConfig"
      :mode="props.mode"
      v-bind="attrs"
      @on-created="handleCreated"
    />
  </div>
</template>
<script lang="ts" setup>
import '@wangeditor/editor/dist/css/style.css'; // 引入 css
import { Editor, Toolbar } from '@wangeditor/editor-for-vue';
import { IDomEditor, i18nChangeLanguage, IEditorConfig } from '@wangeditor/editor';

const props = withDefaults(
  defineProps<{
    mode?: 'default' | 'simple';
    modelValue: string;
    height?: string;
    language?: 'en' | 'zh-CN';
    editorConfig?: Partial<IEditorConfig>;
  }>(),
  {
    mode: 'default',
    modelValue: '',
    height: '300px',
    language: 'en',
    editorConfig: () => ({
      placeholder: 'Please enter content...'
    })
  }
);

// 编辑器实例，必须用 shallowRef
const editorRef = shallowRef<IDomEditor | undefined>(undefined);

// 内容 HTML
const currentValue = computed({
  get: () => props.modelValue,
  set: (val: string) => {
    emits('update:modelValue', val);
  }
});
const emits = defineEmits(['update:modelValue']);

// 获取组件上定义的 listeners
const attrs: any = useAttrs();

const toolbarConfig = {};

watch(
  () => props.language,
  val => {
    i18nChangeLanguage(val);
  },
  { immediate: true }
);

defineExpose({
  editor: editorRef
});
// 组件销毁时，也及时销毁编辑器
onBeforeUnmount(() => {
  const editor = editorRef.value;
  if (editor == null) return;
  editor.destroy();
});

const handleCreated = (editor: IDomEditor) => {
  editorRef.value = editor; // 记录 editor 实例，重要！
};
</script>
<style lang="scss">
.rich-text-editor-content {
  overflow-y: hidden;
  font-size: 16px;
  line-height: 1.5;
}

.rich-text-editor-wrapper {
  border: 1px solid #ccc;
}

.rich-text-editor-toolbar {
  border-bottom: 1px solid #ccc;
}
</style>
