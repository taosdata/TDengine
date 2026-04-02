<template>
  <Explorer v-bind="props" />
</template>

<script lang="ts" setup>
import Explorer from 'components/explorer/index.vue';

type ExplorerProps = InstanceType<typeof Explorer>['$props'];
const props: ExplorerProps = {
  database: {
    isCanCreateDatabase: true
  },
  stable: {},
  table: {}
};
</script>

<style scoped lang="scss"></style>
