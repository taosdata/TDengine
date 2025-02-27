<template>
  <mavonEditor
    ref="md"
    v-model="currentValue"
    :style="{
      height: props.height
    }"
    :language="props.language"
    @img-add="imgAdd"
  />
</template>

<script lang="ts" setup>
import mavon from 'mavon-editor';
import 'mavon-editor/dist/css/index.css';

const props = withDefaults(
  defineProps<{
    modelValue: string;
    height?: string | number;
    limitLength?: number;
    uploadFileFn: (file: FormData) => Promise<Recordable>;
    processUrlFn?: (url: any) => string;
    language?: string;
  }>(),
  {
    modelValue: '',
    height: '300px',
    limitLength: 10000,
    language: 'en',
    processUrlFn: (url: string) => url
  }
);
const emit = defineEmits(['update:modelValue']);
const md = shallowRef<Recordable | null>(null);
const { mavonEditor } = mavon;
const currentValue = computed({
  get: () => props.modelValue,
  set: val => emit('update:modelValue', val)
});

onMounted(() => {
  document
    .querySelector('.no-border.no-resize.auto-textarea-input')
    ?.setAttribute('maxLength', String(props.limitLength));
});

function imgAdd(pos: number, $file: any) {
  const formData = new FormData();
  formData.append('file', $file);
  return props
    .uploadFileFn(formData)
    .then(data => {
      const url = props.processUrlFn(data);
      md.value?.$img2Url(pos, url);
    })
    .catch(() => {
      md.value?.$img2Url(pos, null);
    });
}
</script>

<style scoped lang="scss">
:deep(.no-border.no-resize.auto-textarea-input) {
  background-color: #fff;
}
</style>
