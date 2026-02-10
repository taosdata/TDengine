<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="routeTitle"></PageHeader> -->
    <section class="content">
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
// const route = useRoute();

// const routeTitle = computed(() => {
//   let result = t('dataIn.pageTite');

//   if (route.params?.lang) {
//     result += ' - ' + route.params?.lang;
//   }
//   return result;
// });
</script>

<style lang="scss" scoped>
.content {
  padding-bottom: 0px;
  border: 1px solid #dbdfe6 !important;
  border-radius: 6px;
}
</style>
