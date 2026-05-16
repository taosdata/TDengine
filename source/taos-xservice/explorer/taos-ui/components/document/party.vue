<template>
  <DocsList :docs-list="party" :url-prefix="urlPrefix" />
</template>

<script lang="ts" setup>
import DocsList from './list.vue';
import { party } from './index';

withDefaults(defineProps<{ urlPrefix?: string }>(), {
  urlPrefix: '/dataIn/docs/party/'
});
</script>

<style scoped lang="scss"></style>
