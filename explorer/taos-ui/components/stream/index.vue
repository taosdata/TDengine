<template>
  <div class="page-wrapper">
    <PageHeader :title="t('common.stream')"></PageHeader>
    <section class="content">
      <RouterView />
    </section>
  </div>
</template>

<script lang="ts" setup>
import { t } from 'locales';
</script>

<style lang="scss" scoped></style>
