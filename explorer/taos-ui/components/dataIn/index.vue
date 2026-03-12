<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="routeTitle"></PageHeader> -->
    <section class="content" style="border: none !important">
      <router-view></router-view>
    </section>
  </div>
</template>

<script setup lang="ts">
// import { useRoute } from 'hooks/useCurrentRouter';
// import { t } from 'locales';
import { dataInPropsKey, DataInProps } from './model/useDataIn';
// import PageHeader from '../PageHeader.vue';

const props = defineProps<DataInProps>();
provide(dataInPropsKey, props);
</script>

<style lang="scss" scoped></style>
