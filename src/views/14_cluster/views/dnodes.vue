<template>
  <div class="dnode-block">
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column width="150" :label="$t('topic.end_point')" prop="end_point"></el-table-column>
      <el-table-column width="150" :label="$t('topic.vnodes')" prop="vnodes"></el-table-column>
      <el-table-column width="200" :label="$t('topic.cores')" prop="cores"></el-table-column>
      <el-table-column width="200" :label="$t('topic.status')" prop="status"></el-table-column>
      <el-table-column width="200" :label="$t('topic.role')" prop="role"></el-table-column>
      <el-table-column width="200" :label="$t('topic.create_time')" prop="create_time"></el-table-column>
      <el-table-column width="200" :label="$t('topic.offline_reason')" prop="offline_reason"></el-table-column>
      
      <el-table-column label="Action" width="80">
        <template slot-scope="scope">
          <el-button type="danger" plain size="small" @click="del(scope.row)" icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
  </div>
</template>
<script>
export default {
  data() {
    return {};
  }
};
</script>
<style lang="scss" scoped>
</style>