<template>
  <DocsList :docs-list="party" url-prefix="/dataIn/docs/party/" />
</template>

<script lang="ts" setup>
import DocsList from './list.vue';
import { party } from './index';
</script>

<style scoped lang="scss"></style>
