<template>
  <div id="favorites_wrapper" class="favorites-wrapper">
    <el-tabs v-model="favoriteActiveTab" type="border-card" size="default">
      <el-form :inline="true" size="default" label-position="left" @submit.prevent>
        <el-form-item prop="sql_desc_fuzzy">
          <el-input
            v-model="favoriteParams.sql_desc_fuzzy"
            clearable
            :placeholder="'SQL' + '/' + t('explorer.desc')"
            style="width: 200px"
            @keyup.enter="update"
            @clear="update"
          />
        </el-form-item>
        <el-form-item>
          <el-button icon="Search" @click="update">{{ t('common.search') }}</el-button>
        </el-form-item>
      </el-form>
      <el-tab-pane name="personal" :label="t('explorer.persionalFavorites')">
        <List :list-data="favoriteData.personal" :request-api="itemApi" :total="favoriteData.total" @update="update" />
      </el-tab-pane>
      <el-tab-pane name="shared" :label="t('explorer.sharedFavorites')">
        <List
          :list-data="favoriteData.shared"
          :is-shared="true"
          :request-api="sharedItemApi"
          :total="favoriteData.total"
          @update="update"
        />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { getExplorerProps } from '../../../model/useExplorer';
import { favoriteActiveTab, favoriteData, favoriteParams, updateFavoriteEvent } from '../../utils';
import { t } from 'locales';
import List from './list.vue';

const { favorite } = getExplorerProps();
const itemApi = {
  del: favorite.api.delete,
  edit: favorite.api.edit,
  addShared: favorite.api.addShared
};
const sharedItemApi = {
  del: favorite.api.deleteShared
};

watch(
  favoriteActiveTab,
  val => {
    favoriteParams.page = 1;
    favoriteParams.sql_desc_fuzzy = '';
    if (val == 'personal') {
      delete favoriteParams.is_public;
    } else {
      favoriteParams.is_public = true;
    }
    update();
  },
  {
    immediate: true
  }
);
function update() {
  updateFavoriteEvent.emit();
}
</script>

<style lang="scss" scoped>
.favorites-wrapper {
  height: 100%;

  &:deep(.el-tab-pane) {
    top: 51px !important;
  }

  &:deep(.el-tabs--border-card) {
    box-shadow: none;
  }

  &:deep(.el-table) {
    display: flex;
    flex-direction: column;
    margin-top: 0 !important;
  }

  &:deep(.el-table__header-wrapper) {
    min-height: 30px;
  }

  &:deep(.el-tabs__content > .el-tab-pane) {
    height: 97%;
  }
}
</style>
