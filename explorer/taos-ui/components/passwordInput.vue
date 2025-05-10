<template>
  <el-popover trigger="hover" :width="popoverWidth" placement="right">
    <div v-dompurify-html="t('msg.passwordFormatTip')"></div>
    <template #reference>
      <el-input v-model.trim="value" v-bind="attrs" minlength="8" show-password maxlength="16"></el-input>
    </template>
  </el-popover>
</template>

<script lang="ts" setup>
import { i18n, t } from 'locales';
const popoverWidth = computed(() => {
  return (i18n.global.locale as WritableComputedRef<string>).value === 'en' ? '610px' : '400px';
});
const props = defineProps<{ modelValue: string }>();
const emit = defineEmits(['update:modelValue']);
const value = computed({
  get: () => props.modelValue,
  set: (val: string) => emit('update:modelValue', val)
});

const attrs = useAttrs();
</script>

<style scoped lang="scss">
:deep(ul) {
  list-style: disc;
  list-style-position: inside;
}
</style>
