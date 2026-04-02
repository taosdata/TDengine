<template>
  <Explorer v-bind="props"> </Explorer>
</template>
<script lang="ts" setup>
import Explorer from 'taos-ui/components/explorer/index.vue';
import { getFavorites, addFavorite, delFavorite, manageFavorite } from '@/api/explorer';
import { getDBStruct, deleteDBReq, createDB, updateDB } from '@/api/database';
import { getRunningTask } from '@/api/datain';
import { $IS_COMMUNITY } from '@/utils/init';
const { t } = useI18n();
type Props = InstanceType<typeof Explorer>['$props'];
const currentComponentName = ref('');
const componentKey = ref(0);

const props: Props = {
  database: {
    isCanCreateDatabase: true,
    getDataSourceUsedList: getRunningTask,
    getStructApi: getDBStruct,
    deleteApi: deleteDBReq,
    createApi: createDB,
    updateApi: updateDB
  },
  stable: {},
  table: {},
  pageTitle: t('route.explorer'),
  favorite: {
    api: {
      getList: getFavorites,
      getSharedList: getFavorites,
      add: addFavorite,
      edit: manageFavorite,
      addShared: addFavorite,
      delete: delFavorite,
      deleteShared: delFavorite
    }
  },
  isCloud: false,
  customCompCallback(event: string) {
    componentKey.value++;
    currentComponentName.value = event;
  },
  isCommunity: $IS_COMMUNITY
};
</script>

<style lang="scss" scoped></style>
