<template>
  <div class="activity_table">
    <el-table size="mini" tooltip-effect="light" :data="activityList">
      <el-table-column :label="$t('activity.type')" prop="businessType" width="100"> </el-table-column>
      <el-table-column :label="$t('activity.name')" prop="businessDesc" width="160"> </el-table-column>
      <el-table-column prop="requestTime" :label="$t('activity.requestTime')" width="200">
        <template slot-scope="scope"> {{ parseTime(scope.row.requestTime) }}(UTC) </template>
      </el-table-column>
      <el-table-column prop="operationDesc" :label="$t('activity.operationDesc')" min-width="450" :show-overflow-tooltip="true"> </el-table-column>
      <!-- <el-table-column
        prop="operatorName"
        :label="$t('activity.originator')"
        width="130"
      >
      </el-table-column> -->
      <el-table-column prop="requestIp" :label="$t('activity.requestIp')" width="150"> </el-table-column>
      <el-table-column prop="result" :label="$t('activity.result')" width="150">
        <template slot-scope="scope">
          <el-tag v-if="scope.row.result == 'success'" type="success">{{ $t("success") }}</el-tag>
          <el-tag v-else type="danger">{{ $t("fail") }}</el-tag>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
  </div>
</template>

<script>
  import { mapState } from "vuex";
  import { parseTime } from "@/utils";
  export default {
    computed: {
      ...mapState({
        activityList: state => state.activity.activityList,
        pageSize: state => state.activity.pageSize,
        currentPage: state => state.activity.currentPage,
        total: state => state.activity.total,
      }),
    },
    data() {
      return {};
    },
    methods: {
      handlePageChange(val) {
        this.$store.dispatch("activity/getActivityList", {
          current_page: val,
        });
      },
      parseTime(date) {
        return parseTime(date, "YYYY-MM-DD kk:mm:ss");
      },
    },
  };
</script>

<style scoped>
  .activity_table {
    margin-top: 14px;
  }

  .pagination {
    margin-top: 25px;
    text-align: center;
  }
</style>
