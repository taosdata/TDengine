<template>
  <div>
    <div class="flex-end">
      <el-button plain :disabled="loading" icon="refresh" @click="getConsumers">{{ t('common.refresh') }}</el-button>
    </div>
    <el-table v-loading="loading" :data="dataList">
      <el-table-column label="ID" prop="consumer_id"></el-table-column>
      <el-table-column :label="t('topic.consumerGroup')" prop="consumer_group"></el-table-column>
      <el-table-column :label="t('topic.clientID')" prop="client_id"></el-table-column>
      <el-table-column :label="t('common.status')" prop="status"></el-table-column>
      <el-table-column :label="t('topic.topic')" prop="topics"></el-table-column>
      <el-table-column :label="t('date.upTime')" prop="up_time"></el-table-column>
      <el-table-column :label="t('date.subscribeTime')" prop="subscribe_time"></el-table-column>
      <el-table-column :label="t('date.rebalanceTime')" prop="rebalance_time"></el-table-column>
    </el-table>
    <Pagination
      v-model:current-page="currentPage"
      v-model:page-size="pageSize"
      :total
      @page-change="handlePageChange"
      @size-change="handleSizeChange"
    >
    </Pagination>
  </div>
</template>

<script lang="ts" setup>
import { getConsumers } from '../api';
import usePagination from 'hooks/usePagination';
import { t } from 'locales';

const { currentPage, pageSize, dataList, loading, total, handlePageChange, handleSizeChange } = usePagination({
  getDataFn: getConsumers
});
</script>

<style></style>
