<template>
  <div class="dnode-block">
    <section class="flexBetween">
      <el-form inline size="mini" :disabled="requestIng">
        <section class="flexBetween">
          <div>
            <el-form-item>
              <el-date-picker
                v-model="date"
                size="mini"
                type="daterange"
                :picker-options="pickerOptions"
                range-separator="-"
                :start-placeholder="$t('start')"
                :end-placeholder="$t('end')"
                value-format="timestamp"
                align="left"
              >
              </el-date-picker>
            </el-form-item>
            <el-form-item>
              <el-input
                v-model="filterParams.user_name"
                :placeholder="$t('taosuser.user')"
                @keyup.enter.native="handlePageChange()"
              ></el-input>
            </el-form-item>
            <el-form-item>
              <el-input
                v-model="filterParams.operation"
                :placeholder="$t('taosuser.operation')"
                @keyup.enter.native="handlePageChange()"
              ></el-input>
            </el-form-item>
          </div>
          <el-form-item>
            <el-button icon="el-icon-search" @click="handlePageChange()">{{
              $t("search")
            }}</el-button>
          </el-form-item>
        </section>
      </el-form>
    </section>
    <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng"
        style="font-size: 14px"
        >{{ $t("refresh") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="auditList" size="mini">
      <el-table-column :label="$t('taosuser.time')" prop="ts" width="220">
        <span slot-scope="scope">{{ parsinginZone(scope.row.ts) }}</span>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.user')"
        prop="user_name"
        show-overflow-tooltip
      >
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.operation')"
        prop="operation"
        show-overflow-tooltip
      >
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.details')"
        prop="details"
        min-width="280"
        :show-overflow-tooltip="true"
      >
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.target_1')"
        prop="target_1"
        :show-overflow-tooltip="true"
      >
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.target_2')"
        prop="target_2"
        :show-overflow-tooltip="true"
      >
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
import { sendSQLReq } from "@/api/gateway/console";
import { getAudits } from "@/api/explorer/audit";
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { parsinginZone } from "@/utils";
export default {
  data() {
    return {
      requestIng: false,
      dblist: [],
      pageSize: 20,
      currentPage: 1,
      total: 10,
      auditList: [],
      parsinginZone,
      filterParams: {
        user_name: "",
        operation: "",
      },
      date: [],
      pickerOptions: {
        shortcuts: [
          {
            text: this.$t("yesterday"),
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 1);
              picker.$emit("pick", [start, end]);
            },
          },
          {
            text: this.$t("agoWeek"),
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
              picker.$emit("pick", [start, end]);
            },
          },
          {
            text: this.$t("agoMonth"),
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
              picker.$emit("pick", [start, end]);
            },
          },
        ],
      },
    };
  },
  computed: {},
  methods: {
    handlePageChange() {
      this.getAuditData();
    },
    refresh() {
      this.getAuditData();
    },
    async getAuditData() {
      try {
        if (this.requestIng) return;
        this.requestIng = true;
        let conditions = "";
        if (this.date?.length > 0) {
          conditions = ` ts > ${this.date[0]} AND ts <= ${this.date[1]} AND`;
        }
        const currentFilterParams = { ...this.filterParams };
        for (let key in currentFilterParams) {
          if (!currentFilterParams[key]) {
            delete currentFilterParams[key];
          } else {
            conditions += ` ${key} = '${currentFilterParams[key]}' AND`;
          }
        }
        conditions = conditions.replace(/ AND$/g, "");

        [this.auditList, this.total] = await getAudits({
          currentPage: this.currentPage,
          pageSize: this.pageSize,
          conditions,
        });
        this.requestIng = false;
      } catch (error) {
        console.log("err");
      }
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (err) {
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getDatabases();
    this.getAuditData();
  },
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}
.el-switch {
  margin-right: 10px;
}
</style>
