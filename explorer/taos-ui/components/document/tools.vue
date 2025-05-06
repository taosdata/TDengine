<template>
  <DocsList :docs-list url-prefix="/tools/docs/tool/" />
</template>

<script lang="ts" setup>
import DocsList from './list.vue';
import { tool } from './index';
import { compareVersion } from 'utils/tdengine';
import { project, instance } from 'config';

const docsList = computed(() =>
  tool.filter(item => {
    if (!project.isAliyun && item.isAli) return false;
    if (item.version) {
      return compareVersion(instance.version, item.version);
    } else {
      return true;
    }
  })
);
</script>

<style scoped lang="scss"></style>
