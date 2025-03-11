<template>
  <DocsList :docs-list url-prefix="/program/docs/connector/" />
</template>

<script lang="ts" setup>
import DocsList from './list.vue';
import { connector } from './index';
import { compareVersion } from 'utils/tdengine';
import { instance } from 'config';

const docsList = computed(() =>
  connector.filter(item => {
    if (item.version) {
      return compareVersion(instance.version, item.version);
    } else {
      return true;
    }
  })
);
</script>

<style scoped lang="scss"></style>
