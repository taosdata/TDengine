<template>
  <Condition ref="condRef" v-model="value" v-bind="props" />
  <el-button @click="generateSql">Sql</el-button>
</template>

<script lang="ts" setup>
import Condition from 'components/SqlCondition/index.vue';

type Props = InstanceType<typeof Condition>['$props'];
const value = ref([]);
const condRef = ref<InstanceType<typeof Condition> | null>(null);
const props: Props = {
  fields: [
    {
      field: 'c1',
      type: 'INT'
    },
    {
      field: 'c2',
      type: 'INT'
    }
  ],
  top: true,
  parentField: '',
  isForm: false
};
function generateSql() {
  if (condRef.value) {
    console.log(condRef.value.generateSql(false));
  }
}
</script>

<style scoped lang="scss"></style>
