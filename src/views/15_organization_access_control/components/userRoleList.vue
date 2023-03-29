<template>
  <div class="">
    <el-table size="mini" :data="userRoleList">
      <el-table-column :label="$t('route.orgaization')" width="200" prop="stream_name"></el-table-column>
      <el-table-column label="DB" width="200" prop="create_time"></el-table-column>
      <el-table-column width="100" :label="$t('accessControl.role')" prop="trigger"></el-table-column>
      <el-table-column width="100" :label="$t('accessControl.group')" prop="trigger"></el-table-column>
      <el-table-column :label="$t('accessControl.startDate')" min-width="200" prop="sql"> </el-table-column>
      <el-table-column :label="$t('accessControl.expiration')" min-width="200" prop="sql"> </el-table-column>
      <el-table-column :label="$t('status')" min-width="200" prop="sql"> </el-table-column>

      <el-table-column :label="$t('operate')" width="80">
        <template slot-scope="scope">
          <el-button plain size="small" @click="del(scope.row)" icon="el-icon-delete"></el-button>
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
    >
    </el-pagination>
  </div>
</template>

<script>
  export default {
    name: "",
    mixins: [],
    components: {},
    props: {},
    data() {
      return {
        currentPage: 1,
        pageSize: 10,
        total: 0,
        userRoleList: [],
      };
    },
    computed: {},
    watch: {},
    created() {},
    mounted() {},
    methods: {
      handlePageChange(val) {
        this.currentPage = val;
        this.getUserRoleList();
      },
    },
  };
</script>

<style scoped lang="scss"></style>
