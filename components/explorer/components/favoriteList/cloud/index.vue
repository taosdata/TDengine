<template>
  <el-row class="favorites-wrapper" :gutter="20">
    <el-col :span="isCloud ? 12 : 24">
      <el-card shadow="always">
        <template #header>
          <div>{{ t('explorer.persionalFavorites') }}</div>
        </template>
        <el-empty v-if="favoriteData.personal.length === 0" :image-size="imageSize"></el-empty>
        <template v-else>
          <RecordItem
            v-for="record in favoriteData.personal"
            :key="record.id"
            :record="record"
            :request-api="itemApi"
            @update="update"
          ></RecordItem>
        </template>
      </el-card>
    </el-col>
    <el-col v-if="isCloud" :span="12">
      <el-card shadow="always">
        <template #header>
          <div>{{ t('explorer.sharedFavorites') }}</div>
        </template>
        <el-empty v-if="favoriteData.shared.length === 0" :image-size="imageSize"></el-empty>
        <template v-else>
          <RecordItem
            v-for="record in favoriteData.shared"
            :key="record.id"
            :is-shared="true"
            :is-can-del="isCanDel(record)"
            :record="record"
            :request-api="sharedItemApi"
            @update="update"
          ></RecordItem>
        </template>
      </el-card>
    </el-col>
  </el-row>
</template>

<script lang="ts" setup>
import RecordItem from './item.vue';
import { getExplorerProps } from '../../../model/useExplorer';
import { t } from 'locales';
import { favoriteData, updateFavoriteEvent } from '../../utils';

const imageSize = Math.floor(window.innerHeight / 5);
const { favorite, isCloud } = getExplorerProps();
const itemApi = {
  del: favorite.api.delete,
  addShared: favorite.api.addShared
};
const sharedItemApi = {
  del: favorite.api.deleteShared
};

function update() {
  updateFavoriteEvent.emit();
}
function isCanDel(record: Recordable) {
  if (favorite.isCanDeleteFn) return favorite.isCanDeleteFn(record);
  return true;
}
</script>

<style lang="scss" scoped>
.favorites-wrapper {
  height: 100%;

  &:deep(.el-col) {
    height: 100%;
  }

  &:deep(.el-card) {
    display: flex;
    flex-direction: column;
    height: 100%;
  }

  &:deep(.el-card__body) {
    flex: 1;
    overflow-y: auto;
  }
}
</style>
