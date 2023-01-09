<template>
  <div class="mnode-block">
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column  :label="$t('topic.endpoint')" prop="endpoint"></el-table-column>
      <el-table-column  :label="$t('topic.role')" prop="role"></el-table-column>
      <el-table-column  :label="$t('topic.status')" prop="status"></el-table-column>
      <el-table-column  :label="$t('topic.create_time')" prop="create_time"></el-table-column>
      
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
    return {
      pageSize:10,
      currentPage:1,
      total:10,
      topicList:[]
    };
  },
  methods:{
    handlePageChange(){}
  }
};
</script>
<style lang="scss" scoped>
</style>