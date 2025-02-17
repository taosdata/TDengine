<template>
  <el-dialog v-bind="props.config" v-model="visible" :close-on-click-modal="false" align="center">
    <slot v-if="slots.default"></slot>
    <component
      :is="comp"
      v-else
      :key="props.currentKey"
      style="text-align: left"
      v-bind="props.props"
      @update="emit('update', $event)"
      @close="emit('close', $event)"
      v-on="listeners"
    ></component>
  </el-dialog>
</template>

<script lang="ts" setup>
import { DialogProps } from 'element-plus';
import type { AsyncComponentLoader, Component } from 'vue';

const props = withDefaults(
  defineProps<{
    config: Partial<DialogProps>;
    props?: Record<string, any>;
    comp?: string | AsyncComponentLoader | Component | null;
    listeners?: Recordable;
    currentKey?: number;
    modelValue?: boolean;
  }>(),
  {
    config: () => ({}),
    props: () => ({}),
    listeners: () => ({}),
    comp: '',
    currentKey: 0,
    modelValue: true
  }
);
const emit = defineEmits(['update:modelValue', 'close', 'update']);
const slots = useSlots();
const visible = computed({
  get: () => props.modelValue,
  set: val => emit('update:modelValue', val)
});
const comp = computed(() => {
  const type = typeof props.comp;
  switch (type) {
    case 'function':
      return defineAsyncComponent(props.comp as AsyncComponentLoader);
    case 'object':
      return markRaw(toRaw(props.comp as Component));

    default:
      break;
  }
  return props.comp;
});
</script>

<style scoped lang="scss"></style>
