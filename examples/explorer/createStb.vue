<template>
  <StbForm v-bind="props" />
</template>

<script lang="ts" setup>
import StbForm from 'components/explorer/components/createStable/index.vue';

type Props = InstanceType<typeof StbForm>['$props'];
const props: Props = {
  version: '3.3.0',
  isEdit: false,
  showTitle: true,
  requestApi: (data, db) => {
    return new Promise((resolve: AnyFunction) => {
      console.log(data, db);
      resolve();
    });
  },
  dbData: {
    name: 'test'
  }
};
</script>

<style scoped lang="scss"></style>
