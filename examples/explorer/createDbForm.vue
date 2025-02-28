<template>
  <addDbForm v-bind="props" />
</template>

<script lang="ts" setup>
import addDbForm from 'components/explorer/components/createDbForm.vue';

type Props = InstanceType<typeof addDbForm>['$props'];

const props: Props = {
  dbList: [],
  updateApi: (data: Recordable) =>
    new Promise((resolve: AnyFunction) => {
      console.log(data);
      resolve();
    }),
  isEdit: false,
  version: '3.3.2.7'
};
</script>

<style scoped lang="scss"></style>
