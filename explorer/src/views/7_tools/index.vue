<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="routeTitle"></PageHeader> -->
    <div class="content">
      <router-view />
    </div>
  </div>
</template>

<script setup lang="ts">
// import type { RouteLocationNormalizedLoadedGeneric } from 'vue-router';
// const { t } = useI18n();
// const route: RouteLocationNormalizedLoadedGeneric = useRoute();

// const routeTitle = computed(() => {
//   let result = t('route.tool');
//   const YonghongBI = t('docs.tools.yonghongbi.name');

//   if (route.params?.lang) {
//     if (['YonghongBI'].includes(route?.params?.lang)) {
//       result += ' - ' + YonghongBI;
//     } else {
//       result += ' - ' + route.params.lang;
//     }
//   }
//   return result;
// });
</script>

<style lang="scss" scoped></style>
