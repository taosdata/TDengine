<template>
  <Condition v-model="currentValue" :fields="props.fields" />
</template>

<script lang="ts" setup>
import Condition from './condition.vue';
import { DataItem, parseWhereCondition, generateConditionString, Field } from './utils';

const props = defineProps<{
  modelValue: string;
  fields: Field[];
}>();
const currentValue = ref<DataItem[]>([]);
const emits = defineEmits(['update:modelValue']);
let newValue = '';

watch(
  () => props.modelValue,
  val => {
    if (val == newValue) return;
    currentValue.value = parseWhereCondition(val);
  },
  { immediate: true }
);

watch(
  currentValue,
  val => {
    newValue = generateConditionString(val, props.fields, false);
    emits('update:modelValue', newValue);
  },
  { deep: true }
);
</script>

<style scoped lang="scss"></style>
