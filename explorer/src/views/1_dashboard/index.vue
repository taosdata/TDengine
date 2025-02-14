<template>
  <div class="page-wrapper">
    <PageHeader :title="$t('dashboard.overview')"></PageHeader>
    <div class="content">
      <docs :category="'dashboard'" :lang="'Dashboard'"></docs>
    </div>
  </div>
</template>

<script setup lang="ts">
import Docs from '@/components/document/docs.vue';
</script>

<style lang="scss" scoped></style>
