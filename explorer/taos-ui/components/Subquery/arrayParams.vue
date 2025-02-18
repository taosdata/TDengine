<template>
  <div class="">
    <div v-for="(_, index) in props.modelValue" :key="index">
      <FromItem v-model="currentValue[index]" :config="props.config" />
      <el-button icon="delete" @click="del(index)"></el-button>
    </div>
    <el-button class="w-full" icon="plus" plain @click="add"></el-button>
  </div>
</template>

<script lang="ts" setup>
import { FnFilterItem } from 'constants1';
import FromItem from './formItem.vue';

const props = defineProps<{
  config: FnFilterItem;
  modelValue: any[];
}>();

const currentValue = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emits('update:modelValue', val);
  }
});
const emits = defineEmits(['update:modelValue']);
function add() {
  currentValue.value.push('');
}

function del(index: number) {
  currentValue.value.splice(index, 1);
}
</script>

<style scoped lang="scss"></style>
