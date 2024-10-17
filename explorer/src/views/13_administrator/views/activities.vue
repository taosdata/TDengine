<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        style="font-size:14px;"
        >{{ $t("refresh") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini" :show-header="false">
      <el-table-column
      :label="$t('topic.time')"
        prop="time"
      ></el-table-column>
      <el-table-column
      :label="$t('topic.name')"
        prop="name"
      ></el-table-column>
      <el-table-column
        prop="desc"
        :label="$t('topic.desc')"
      ></el-table-column>

      <el-table-column label="Action" width="65">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
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
      pageSize: 10,
      currentPage: 1,
      total: 10,
      topicList: [
        {
            time:'2022-12-28 15:06:00.098',
            name:'John',
            desc:'John123@gmail.com',
        },
      ],
    };
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + '?', "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancel",
        type: "warning",
      });
    },
    refresh(){
    }
  },
};
</script>
<style lang="scss" scoped>
</style>