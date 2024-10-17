<template>
  <div class="slowTable">
    <el-table :data="slowList" size="mini" header-cell-class-name="header-class" style="width: 100%">
      <el-table-column align="center" prop="timeCost" width="65" :label="$t('support.status')">
        <template slot-scope="scope">
          <Icon name="status" class="status_icon" :style="{ color: status[scope.row.success] }"></Icon>
        </template>
      </el-table-column>

      <el-table-column align="center" width="220">
        <template slot="header">
          <el-tooltip class="item" effect="dark" :content="$t('sql.endTimeTip')" placement="top-start">
            <span>
              <span>{{ $t("sql.endTime") }}</span>
              <el-icon style="margin-left: 5px" class="el-icon-warning-outline" />
            </span>
          </el-tooltip>
        </template>
        <template slot-scope="scope"> {{ scope.row.ts }}(UTC) </template>
      </el-table-column>
      <el-table-column align="center" label="SQL" min-width="300">
        <template slot-scope="scope">
          <pre v-highlight class="nowrap sql-code pre-code" slot="reference">
          <code class="language-sql" style="overflow:hidden">{{ scope.row.sqlStatement }} </code>
        </pre>
        </template>
      </el-table-column>
      <el-table-column align="center" width="200" prop="timeCost">
        <template slot="header">
          <el-tooltip class="item" effect="dark" :content="$t('sql.totalTimeTip')" placement="top-start">
            <span>
              <span>{{ $t("sql.totalTime") }} (ms)</span>
              <el-icon style="margin-left: 5px" class="el-icon-warning-outline" />
            </span>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      small
      layout="total, prev, pager, next"
      class="pagination"
      :hide-on-single-page="true"
      :page-size="pageSize"
      :current-page="currentPage"
      :total="total"
      @current-change="handleChangePage"
    >
    </el-pagination>
  </div>
</template>

<script>
  import { mapState } from "vuex";
  export default {
    data() {
      return {
        status: {
          success: "#67c23a",
          fail: "#f56c6c",
          running: "#409eff",
        },
      };
    },
    computed: {
      ...mapState({
        slowList: state => state.slow.slowList,
        currentPage: state => state.slow.currentPage,
        pageSize: state => state.slow.pageSize,
        total: state => state.slow.total,
      }),
    },
    methods: {
      handleChangePage(val) {
        this.$store.dispatch("slow/getSlowSqlList", val);
      },
    },
  };
</script>

<style lang="scss" scoped>
  .slowTable {
    margin-top: 14px;
  }

  .sqlstatement {
    font-weight: 500;
    color: #333;
  }

  .pagination {
    margin-top: 25px;
    text-align: center;
  }
  .status_icon {
    width: 35px;
    height: 35px;
  }
</style>
<style lang="scss">
  .sql-code {
    position: relative;
    text-align: left;
    padding: 3px 0;
  }
  .header-class {
    font-weight: 500;
  }
</style>
