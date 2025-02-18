<template>
  <div>
    <div class="flex-end">
      <el-button
        plain
        type="primary"
        size="default"
        icon="Refresh"
        :disabled="requestIng"
        style="font-size: 14px"
        @click="refresh"
        >{{ $t('refresh') }}</el-button
      >
    </div>
    <el-table :data="consumerList" style="margin-top: 20px">
      <el-table-column :label="$t('topic.consumerID')" prop="consumer_id" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('topic.consumerGroup')" prop="consumer_group" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('topic.clientID')" prop="client_id" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('status')" prop="status" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('route.topic')" prop="topics" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('topic.upTime')" prop="up_time" show-overflow-tooltip>
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.up_time) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('topic.subscribeTime')" prop="subscribe_time" show-overflow-tooltip>
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.subscribe_time) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('topic.rebalanceTime')" prop="rebalance_time" show-overflow-tooltip>
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.rebalance_time) }}</span>
        </template>
      </el-table-column>
      <!-- <el-table-column label="Pid" prop="pid"></el-table-column> -->
      <!-- <el-table-column :label="$t('topic.endPoint')" prop="end_point"></el-table-column> -->
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      class="pagination"
      layout="total, prev, pager, next"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
  </div>
</template>

<script setup lang="ts">
import { getConsumers } from '@/api/topic';
import { parsinginZone } from '@/utils';

const consumerList = ref([]);
const requestIng = ref<boolean>(false);
const currentPage = ref(1);
const pageSize = ref(10);
const total = ref(0);

function refresh() {
  getConsumersData();
}
async function getConsumersData() {
  if (requestIng.value) return;
  requestIng.value = true;
  [consumerList.value, total.value] = await getConsumers({ currentPage: currentPage.value, pageSize: pageSize.value });
  requestIng.value = false;
}
// function createSubscribe() {
//   if (requestIng.value) return;
//   requestIng.value = true;
// }
function handlePageChange() {
  getConsumersData();
}
getConsumersData();
</script>

<style></style>
