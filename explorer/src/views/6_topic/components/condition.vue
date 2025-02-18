<template>
  <ul class="condition-list">
    <li v-for="(item, index) in condition" :key="item.key">
      <el-select v-model="item.operator" placeholder="Operator" @change="() => changeOperator(item)">
        <template #prepend></template>
        <el-option
          v-for="ite in conditionList"
          :key="ite"
          :disabled="getSelectableCondition(ite)"
          :value="ite"
        ></el-option>
      </el-select>
      <span v-if="['BETWEEN', 'NOT BETWEEN'].includes(item.operator)" class="condition-span">
        <el-input v-model="item.value" placeholder=""></el-input>
        <span>AND</span>
        <el-input v-model="item.value1" placeholder=""></el-input>
      </span>
      <span v-else class="condition-span">
        <el-input
          v-model="item.value"
          placeholder=""
          :disabled="['IS NULL', 'IS NOT NULL'].includes(item.operator)"
        ></el-input>
      </span>
      <el-button icon="Minus" @click="del(index)"></el-button>
    </li>
    <li class="add-btn">
      <el-button class="w100" icon="Plus" @click="addCondition"></el-button>
    </li>
  </ul>
</template>

<script setup lang="ts">
const props = defineProps<{
  condition: Array<Recordable>;
  conditionList: Array<Recordable>;
}>();
const emit = defineEmits(['update:condition']);

const currentOperatorList = computed(() => {
  return props.condition.map(item => item.operator);
});

function del(index) {
  // eslint-disable-next-line vue/no-mutating-props
  props.condition.splice(index, 1);
}
function addCondition() {
  // eslint-disable-next-line vue/no-mutating-props
  props.condition.push({
    key: Date.now(),
    operator: '',
    value: '',
    value1: ''
  });
}
function getSelectableCondition(operator) {
  switch (operator) {
    case '>':
      return currentOperatorList.value.includes('>=') || currentOperatorList.value.includes('>');
    case '>=':
      return currentOperatorList.value.includes('>') || currentOperatorList.value.includes('>=');
    case '<':
      return currentOperatorList.value.includes('<=') || currentOperatorList.value.includes('<');
    case '<=':
      return currentOperatorList.value.includes('<') || currentOperatorList.value.includes('<=');

    default:
      break;
  }
}
function changeOperator(options) {
  options.value = '';
  options.value1 = '';
  emit('update:condition');
}
</script>

<style scoped lang="scss">
.condition-list {
  max-height: 300px;
  overflow-y: auto;

  &:deep(.el-select .el-input) {
    width: 120px;
  }

  li {
    display: flex;
  }

  li + li {
    margin-top: 10px;
  }

  .condition-span {
    display: flex;
    width: 206px;

    span {
      flex: none;
      padding: 0 2px;
      line-height: 32px;
    }
  }

  .add-btn {
    position: sticky;
    bottom: 0;
  }
}
</style>
