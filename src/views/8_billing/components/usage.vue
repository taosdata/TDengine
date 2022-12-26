<template>
  <div class="usage">
    <section class="flexBetween">
      <p class="tip">{{ $t("billing.usageTip") }}</p>
      <el-date-picker
        size="mini"
        type="daterange"
        range-separator="—"
        :start-placeholder="$t('start')"
        :end-placeholder="$t('end')"
        :picker-options="$root.pickerOptions"
        align="left"
        value-format="timestamp"
        v-model="useDate"
        @change="handleChange"
      >
      </el-date-picker>
    </section>
    <el-table size="mini" stripe style="margin-top: 20px" :data="useData">
      <el-table-column width="140" :label="$t('date')">
        <template slot-scope="{ row }">
          {{ row.startTime | handleDate }}
        </template>
      </el-table-column>
      <el-table-column min-width="200" prop="clusterAlias" :label="$t('currentCluster')"> </el-table-column>
      <el-table-column min-width="120" prop="cloudName" :label="$t('dashboard.cloud')"> </el-table-column>
      <el-table-column min-width="120" prop="regionName" :label="$t('dashboard.region')"> </el-table-column>
      <el-table-column min-width="160" prop="ingress" :label="$t('dashboard.ingressVolume') + '(MB)'"></el-table-column>
      <el-table-column min-width="160" prop="egress" :label="$t('dashboard.egressVolume') + '(MB)'"></el-table-column>
      <el-table-column min-width="140" prop="storage" :label="$t('dashboard.storage') + '(GB Hour)'"></el-table-column>
      <el-table-column min-width="100" prop="insertCount" :label="$t('dashboard.insert')"></el-table-column>
      <el-table-column min-width="100" prop="queryCount" :label="$t('dashboard.query')"></el-table-column>
      <el-table-column min-width="100" prop="cost" :label="$t('billing.cost') + '($)'"></el-table-column>
    </el-table>
    <el-pagination
      small
      layout="total, prev, pager, next"
      class="pagination"
      :hide-on-single-page="true"
      :page-size="pageSize"
      :current-page.sync="currentPage"
      :total="total"
      @current-change="getData"
    >
    </el-pagination>
  </div>
</template>

<script>
  import { getUsage } from "api/billing";
  import { OFFSETUTCTIME } from "@/const";
  import moment from "moment";
  const endTimeSuffix = 86399999;
  export default {
    data() {
      return {
        useDate: [],
        useData: [],
        total: 0,
        currentPage: 1,
        pageSize: 10,
      };
    },
    filters: {
      handleDate(value) {
        if (!value) return "";
        return moment.utc(Number(value)).format("YYYY-MM-DD") || "";
      },
    },
    created() {
      this.getData();
    },
    methods: {
      getData() {
        getUsage({
          startTime: this.useDate[0] ? this.useDate[0] - OFFSETUTCTIME : null,
          endTime: this.useDate[1] ? this.useDate[1] - OFFSETUTCTIME + endTimeSuffix : null,
          currentPage: this.currentPage,
          pageSize: this.pageSize,
        })
          .then(({ total, content }) => {
            this.total = total;
            this.useData = content;
          })
          .catch(() => {
            this.total = 0;
            this.useData = [];
          });
      },
      handleChange(val) {
        if (!val) {
          this.useDate = [];
        }
        this.currentPage = 1;
        this.getData();
      },
      handleChangePage() {},
    },
  };
</script>

<style scoped lang="scss"></style>
