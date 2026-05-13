<template>
  <div id="favorites_wrapper" class="favorites-wrapper">
    <div class="favorites-header" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 12px 0px 12px;">
      <el-radio-group v-model="favoriteActiveTab" size="default">
        <el-radio label="personal">{{ t('explorer.persionalFavorites') }}</el-radio>
        <el-radio label="shared">{{ t('explorer.sharedFavorites') }}</el-radio>
      </el-radio-group>
      <el-form :inline="true" size="default" label-position="left" @submit.prevent>
        <el-form-item prop="sql_desc_fuzzy" style="margin-right: 10px; margin-bottom: 0px;">
          <el-input
            v-model="favoriteParams.sql_desc_fuzzy"
            clearable
            :placeholder="'SQL' + '/' + t('explorer.desc')"
            style="width: 200px"
            @keyup.enter="update"
            @clear="update"
          />
        </el-form-item>
        <el-form-item style="margin-right: 0px; margin-bottom: 0px;">
          <el-button icon="Search" @click="update">{{ t('common.search') }}</el-button>
        </el-form-item>
      </el-form>
    </div>
    <div class="favorites-content" style="padding: 14px;">
      <List
        v-if="favoriteActiveTab === 'personal'"
        :list-data="favoriteData.personal"
        :request-api="itemApi"
        :total="favoriteData.total"
        @update="update"
      />
      <List
        v-if="favoriteActiveTab === 'shared'"
        :list-data="favoriteData.shared"
        :is-shared="true"
        :request-api="sharedItemApi"
        :total="favoriteData.total"
        @update="update"
      />
    </div>
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

  &:deep(.el-radio__label) {
    color: #303133;
  }

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
    height: 90%;
    padding-left: 14px;
    padding-right: 14px;
  }
}
</style>
