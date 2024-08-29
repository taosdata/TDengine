<template>
  <div class="dnode-block">
    <section class="flexBetween">
      <el-form inline size="mini" :disabled="requestIng">
        <section class="flexBetween">
          <div>
            <el-form-item>
              <TimezoneDatePicker
                v-model="date"
                size="mini"
                type="datetimerange"
                :picker-options="pickerOptions"
                range-separator="-"
                :start-placeholder="$t('start')"
                :end-placeholder="$t('end')"
                value-format="timestamp"
                align="left"
              >
              </TimezoneDatePicker>
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
            <el-button icon="el-icon-search" @click="handlePageChange()" :disabled="$COMMUNITY">{{
              $t("search")
            }}</el-button>
          </el-form-item>
          <el-form-item>
            <el-button @click="handlePageReset()" >{{ $t("reset") }}</el-button>
          </el-form-item>
        </section>
      </el-form>
      <div style="margin-bottom: 18px">
        <el-tooltip
          placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
        >
          <template slot="content">
            <span v-html="$t('communityTip')"></span>
          </template>
          <el-button :disabled="requestIng || $COMMUNITY" @click="exportFile" size='mini' type="primary" plain
            >{{ $t("console.export") }}
          </el-button>
        </el-tooltip>
      </div>
    </section>
    <!-- <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng"
        style="font-size: 14px"
        >{{ $t("refresh") }}</el-button
      >
    </div> -->
    <el-table style="margin-top: 20px" :data="auditList" size="mini">
      <el-table-column :label="$t('taosuser.time')" prop="ts" width="220">
        <span slot-scope="scope">{{ parsinginZone(scope.row.ts) }}</span>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.clientAddress')"
        prop="client_address"
        width="180"
      >
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.client_address" placement="top-start">
            <span class="nowrap">{{ scope.row.client_address }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.user')"
        prop="user_name"
      >
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.user_name" placement="top-start">
            <span class="nowrap">{{ scope.row.user_name }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.operation')"
        prop="operation"
      >
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.operation" placement="top-start">
            <span class="nowrap">{{ scope.row.operation }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.db')"
        prop="db"
      >
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.db" placement="top-start">
            <span class="nowrap">{{ scope.row.db }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.resource')"
        prop="resource"
      >
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.resource" placement="top-start">
            <span class="nowrap">{{ scope.row.resource }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('taosuser.details')"
        prop="details"
        min-width="260"
      >
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.details" placement="top-start">
            <span class="nowrap">{{ scope.row.details }}</span>
          </el-tooltip>
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
import { sendSQLReq } from "@/api/gateway/console";
import { getAudits } from "@/api/explorer/audit";
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { parsinginZone } from "@/utils";
import { parse } from "json2csv";
import FileSaver from "file-saver";
import { auditMockData } from "@/const"
export default {
  components: {
    TimezoneDatePicker: () => import("@/components/date-picker"),
  },
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
      exportAuditList: [],
    };
  },
  props: {
    activeName: {
      type: String,
      default: ''
    }
  },
  computed: {
    pickerOptions() {
      return {
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
      };
    },
    conditions() {
      let conditions = "";
      if (this.date?.length > 0) {
        let start = parsinginZone(this.date[0])
        let end = parsinginZone(this.date[1])
        conditions = ` ts > to_unixtimestamp('${start}') AND ts <= to_unixtimestamp('${end}') AND`;
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
      return conditions;
    },
  },
  methods: {
    handlePageChange() {
      if (!this.$COMMUNITY) {
        this.getAuditData();
      }
    },
    refresh() {
      this.getAuditData();
    },
    handlePageReset() {
      if (!this.$COMMUNITY) {
        Object.assign(this.$data, this.$options.data());
        this.getAuditData();
      }
    },
    async getAuditData() {
      try {
        if (this.requestIng) return;
        this.requestIng = true;

        [this.auditList, this.total] = await getAudits({
          currentPage: this.currentPage,
          pageSize: this.pageSize,
          conditions: this.conditions,
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
    async getAllAuditData() {
      let countRes = await sendSQLReq(
        `select count(*) from audit.operations ${
          this.conditions ? "where" + this.conditions : ""
        }`
      );
      let pageSize = countRes?.code == 0 ? countRes.data[0][0] : 0;
      
      let res = await sendSQLReq(
        `select * from audit.operations ${
          this.conditions ? "where" + this.conditions : ""
        }`
      );
      if (res.data && res.data.length > 0) {
        return res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
      } else {
        return Object.fromEntries(
          res.column_meta.map((item, index) => {
            return [item[0], ''];
          })
        )
      }

    },
    async exportFile() {
      let exportAuditList = await this.getAllAuditData();
      const FileName = "audit.csv";
      const data = parse(exportAuditList);
      const blob = new Blob(["\uFEFF" + data], {
        type: "text/csv;charset=utf-8;",
      });
      FileSaver.saveAs(blob, FileName);
    },
  },
  watch: {
    activeName(val) {
      if (val == 'audit' && !this.$COMMUNITY) {
        this.getDatabases();
        this.getAuditData();
      }
    }
  },
  created() {
    if (this.$COMMUNITY) {
      this.auditList = auditMockData
    } else {
      this.getDatabases();
      this.getAuditData();
    }
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
