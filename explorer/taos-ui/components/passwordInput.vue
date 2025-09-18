<template>
  <el-popover trigger="hover" :width="popoverWidth" :placement="placement">
    <div v-dompurify-html="passwordFormat"></div>
    <template #reference>
      <el-input v-model.trim="value" v-bind="attrs" minlength="8" show-password maxlength="16"></el-input>
    </template>
  </el-popover>
</template>

<script lang="ts" setup>
import { i18n, t } from 'locales';
import type { Placement } from 'element-plus';
const popoverWidth = computed(() => {
  return (i18n.global.locale as WritableComputedRef<string>).value === 'en' ? '628px' : '430px';
});
const props = withDefaults(defineProps<{ modelValue: string; placement?: Placement }>(), {
  modelValue: '',
  placement: 'right'
});
const emit = defineEmits(['update:modelValue']);
const value = computed({
  get: () => props.modelValue,
  set: (val: string) => emit('update:modelValue', val)
});
const passwordFormat = computed(() => {
  return t('msg.passwordFormatTip');
});

const attrs = useAttrs();
</script>

<style scoped lang="scss"></style>
