<template>
  <div>
    <el-tabs activeName="log_desc">
      <el-tab-pane :label="$t('slowSql.tab1')" name="log_desc">
        <div class="dnode-block">
          <section class="flexBetween">
            <el-form :inline="true" size="mini" :disabled="requestIng" label-position="left" :rules="rules">
              <!-- <section class="flexBetween"> -->
                <!-- <div> -->
                  <el-form-item
                   :label="$t('slowSql.startTs')"
                  >
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
                      style="width: 320px"
                    >
                    </TimezoneDatePicker>
                  </el-form-item>
                  <el-form-item
                    :label="$t('slowSql.queryTime')"
                    prop="query_time"
                   >
                   <el-input-number
                      v-model="filterParams.query_time_1"
                      style="width: 70px"
                      placeholder="[min"
                      :min="0"
                      :controls="false"
                      :precision="1"
                    /> -
                    <el-input-number 
                      v-model="filterParams.query_time_2"
                      style="width: 70px"
                      placeholder="max]"
                      :min="0"
                      :controls="false"
                      :precision="1"
                    />
                  </el-form-item>
                  <el-form-item
                    :label="$t('slowSql.deDuplication')"
                   >
                   <el-switch
                      v-model="de_duplication"
                    />
                  </el-form-item>
                <!-- </div> -->
                <el-form-item>
                  <el-button
                    icon="el-icon-search"
                    @click="handlePageChange()"
                    >{{ $t("search") }}</el-button
                  >
                </el-form-item>
                <el-form-item>
                  <el-button @click="handlePageReset()">{{
                    $t("reset")
                  }}</el-button>
                </el-form-item>
              <!-- </section> -->
            </el-form>
            <div style="margin-bottom: 18px">
              <!-- <el-popover
                placement="left-start"
                width="460"
                v-model="visible">
                <el-form inline size="small" :disabled="requestIng" label-width="180px" label-position="left" style="padding: 10px">
                  <el-form-item
                    label="slowLogScope"
                    class="flex"
                    v-for="config in configData"
                    :key="config.name"
                  >
                  <template slot="label">
                    <el-tooltip placement="top" effect="light" :open-delay="0">
                      <template slot="content">
                        <DocsContent
                          :content="config.description"
                        />
                      </template>
                      <span>
                        <span>{{ config.display }}</span>
                        <span style="margin-left: 1px">
                          <Icon name="label_info" class="info_icon_custom"></Icon>
                        </span>
                      </span>
                    </el-tooltip>
                  </template>
                  <div class="before">
                    <el-input
                      v-if="config.type == 'input'"
                      v-model="data[config.name]"
                      class="ds-select"
                    ></el-input>
                    <el-input-number
                      v-if="config.type == 'number'"
                      v-model="data[config.name]"
                      :max="config.max"
                      :min="config.min"
                      :placeholder="config.placeholder"
                    ></el-input-number>
                    <el-select
                      v-if="config.type == 'select'"
                      v-model="data[config.name]"
                      class="ds-select"
                      clearable
                      :placeholder="config.placeholder"
                      :multiple="config.multiple"
                    >
                      <el-option
                        v-for="item in config.choices"
                        :key="item"
                        :label="item"
                        :value="item"
                      ></el-option>
                    </el-select>
                    <el-switch
                      v-if="config.type == 'switch'"
                      v-model="data[config.name]"
                      :placeholder="config.placeholder"
                    ></el-switch>
                  </div>
                  <el-button
                    icon="el-icon-check"
                    size="small"
                    class="end"
                  ></el-button>
                  </el-form-item>
                </el-form>
                <div style="text-align: right; margin: 0">
                  <el-button size="mini" type="text" @click="visible = false">取消</el-button>
                </div>
                <el-button
                  slot="reference"
                  :disabled="requestIng || $COMMUNITY"
                  size="small"
                  type="primary"
                  plain
                  style="margin-right: 10px"
                  >{{ $t("route.setting") }}
                </el-button>
              </el-popover> -->
              <el-button
                :disabled="requestIng"
                @click="exportFile"
                size="mini"
                type="primary"
                plain
                >{{ $t("slowSql.exportingSlowLogs") }}
              </el-button>
            </div>
          </section>
          <el-table style="margin-top: 20px" :data="slowSqlLogList" size="mini" @sort-change="customSort">
            <el-table-column :label="$t('slowSql.startTs')" prop="start_ts" width="220">
              <span slot-scope="scope">{{ parsinginZone(scope.row.start_ts) }}</span>
            </el-table-column>
            <el-table-column :label="$t('slowSql.sql')" prop="sql" min-width="180">
              <template slot-scope="scope">
                <el-tooltip
                  placement="left-start"
                  :content="scope.row.sql"
                  popper-class="my-popper"
                  :open-delay="1000"
                >
                  <span>
                    <pre v-highlight class="nowrap sql-code pre-code" slot="reference">
                      <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                    </pre>
                  </span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.db')" prop="db">
              <template slot-scope="scope">
                <el-tooltip :content="scope.row.db" placement="top-start">
                  <span class="nowrap">{{ scope.row.db }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.ip')" prop="ip">
              <template slot-scope="scope">
                <el-tooltip :content="scope.row.ip" placement="top-start">
                  <span class="nowrap">{{ scope.row.ip }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.user')" prop="user">
              <template slot-scope="scope">
                <el-tooltip :content="scope.row.user" placement="top-start">
                  <span class="nowrap">{{ scope.row.user }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.queryTime')" prop="query_time" sortable="custom" width="160px" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(numToFixed(scope.row.query_time))" placement="top-start">
                  <span class="nowrap">{{ numToFixed(scope.row.query_time) }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.rowsNum')" prop="rows_num" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(scope.row.rows_num)" placement="top-start">
                  <span class="nowrap">{{ scope.row.rows_num }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
          <el-pagination
            class="pagination"
            layout="sizes, total, prev, pager, next"
            :current-page.sync="currentPage"
            :page-sizes="[20, 50, 100, 200]"
            :page-size="pageSize"
            :hide-on-single-page="false"
            :total="total"
            @size-change="handleSizeChange"
            @current-change="handlePageChange"
          ></el-pagination>
        </div>
      </el-tab-pane>
      <el-tab-pane :label="$t('slowSql.tab2')" name="statistics">
        <div class="dnode-block">
          <section class="flexBetween">
            <el-form inline size="mini" :disabled="requestIng">
              <section class="flexBetween">
                <div>
                  <el-form-item
                   :label="$t('slowSql.startTs')"
                  >
                    <TimezoneDatePicker
                      v-model="date_two"
                      size="mini"
                      type="datetimerange"
                      :picker-options="pickerOptions"
                      range-separator="-"
                      :start-placeholder="$t('start')"
                      :end-placeholder="$t('end')"
                      value-format="timestamp"
                      align="left"
                      style="width: 320px"
                    >
                    </TimezoneDatePicker>
                  </el-form-item>
                 
                </div>
                <el-form-item>
                  <el-button
                    icon="el-icon-search"
                    @click="handlePageChangeTwo()"
                    >{{ $t("search") }}</el-button
                  >
                </el-form-item>
                <el-form-item>
                  <el-button @click="handlePageReset('tab2')">{{
                    $t("reset")
                  }}</el-button>
                </el-form-item>
              </section>
            </el-form>
            <div style="margin-bottom: 18px">
              <!-- <el-tooltip
                placement="top"
                effect="light"
                :open-delay="0"
                :disabled="!$COMMUNITY"
              >
                <template slot="content">
                  <span v-html="$t('communityTip')"></span>
                </template>
                <el-button
                  :disabled="requestIng || $COMMUNITY"
                  @click="exportFile"
                  size="mini"
                  type="primary"
                  plain
                  >{{ $t("slowSql.exportingSlowLogs") }}
                </el-button>
              </el-tooltip> -->
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
          <el-table style="margin-top: 20px" :data="statisticsList" size="mini"  @sort-change="customSort">
            <el-table-column :label="$t('slowSql.sql')" prop="sql" min-width="180">
              <template slot-scope="scope">
                <el-tooltip
                  placement="left-start"
                  :content="scope.row.sql"
                  popper-class="my-popper"
                  :open-delay="1000"
                >
                  <span>
                    <pre v-highlight class="nowrap sql-code pre-code" slot="reference">
                      <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                    </pre>
                  </span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.db')" prop="db">
              <template slot-scope="scope">
                <el-tooltip :content="scope.row.db" placement="top-start">
                  <span class="nowrap">{{ scope.row.db }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.executionTimes')" prop="query_count" width="130px" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(scope.row.query_count)" placement="top-start">
                  <span class="nowrap">{{ scope.row.query_count }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.averageTime')" prop="avg_query_time" width="200px" sortable="custom" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(numToFixed(scope.row.avg_query_time))" placement="top-start">
                  <span class="nowrap">{{ numToFixed(scope.row.avg_query_time) }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.maximumTime')" prop="max_query_time" width="200px" sortable="custom" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(numToFixed(scope.row.max_query_time))" placement="top-start">
                  <span class="nowrap">{{ numToFixed(scope.row.max_query_time) }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.averageRow')" prop="avg_rows_num" width="130px" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(scope.row.avg_rows_num)" placement="top-start">
                  <span class="nowrap">{{ scope.row.avg_rows_num }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.maximumRow')" prop="max_rows_num" width="130px" align="right">
              <template slot-scope="scope">
                <el-tooltip :content="String(scope.row.max_rows_num)" placement="top-start">
                  <span class="nowrap">{{ scope.row.max_rows_num }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
          <el-pagination
            class="pagination"
            layout="sizes, total, prev, pager, next"
            :page-sizes="[20, 50, 100, 200]"
            :current-page.sync="currentPageTwo"
            :page-size="pageSizeTwo"
            :hide-on-single-page="false"
            :total="totalTwo"
            @size-change="handleSizeChangeTwo"
            @current-change="handlePageChangeTwo"
          ></el-pagination>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>

</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { parsinginZone } from "@/utils";
import { parse } from "json2csv";
import FileSaver from "file-saver";
import { slowSqlMockData } from "@/const";
import { getDataConfig, getSlowSqlLogs, getSlowSqlStatistics } from '@/api/explorer/slowSql';
export default {
  components: {
    DocsContent: () => import("@/views/support/components/editorContentDisplay.vue"),
    TimezoneDatePicker: () => import("@/components/date-picker"),
  },
  data() {
    return {
      requestIng: false,
      dblist: [],
      pageSize: 20,
      total: 10,
      currentPage: 1,
      pageSizeTwo: 20,
      currentPageTwo: 1,
      totalTwo: 10,
      slowSqlLogList: [],
      statisticsList: [],
      parsinginZone,
      filterParams: {
        query_time_1: 10,
        query_time_2: undefined,
      },
      de_duplication: false,
      date: [new Date().getTime() - 3600 * 1000 * 24 * 1, new Date().getTime()],
      date_two: [],
      exportAuditList: [],
      visible: false,
      configData: [],
      data: {},
      query_time_sort: null,
      orderSql: null,
      rules: {
        query_time: [
          {
            validator: this.checkQueryTime,
            trigger: "blur",  
          }
        ],
      },
    };
  },
  props: {
    activeName: {
      type: String,
      default: "",
    },
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
        conditions = ` start_ts > ${this.date[0]} AND start_ts <= ${this.date[1]} AND`;
      }
      const { query_time_1, query_time_2 } = this.filterParams
      if (query_time_1) {
        conditions += ` query_time >= ${query_time_1 * 1000} AND`;
      }
      if (query_time_2) {
        conditions += ` query_time <= ${query_time_2 * 1000} AND`;
      }
      conditions = conditions.replace(/ AND$/g, "");
      return conditions;
    },
    conditions_two() {
      let conditions = "";
      if (this.date_two?.length > 0) {
        conditions = ` start_ts > ${this.date_two[0]} AND start_ts <= ${this.date_two[1]} AND`;
      }
      conditions = conditions.replace(/ AND$/g, "");
      return conditions;
    },
  },
  methods: {
    handlePageChange() {
      this.getSlowSqlLogData();
    },
    handleSizeChange(val) {
      this.pageSize = val;
      this.getSlowSqlLogData();
    },
    handlePageChangeTwo() {
      this.getStatisticsData();
    },
    handleSizeChangeTwo(val) {
      this.pageSizeTwo = val;
      this.getStatisticsData();
    },
    refresh() {
      this.getSlowSqlLogData();
    },
    handlePageReset(tab) {
      Object.assign(this.$data, this.$options.data());
      if (tab == 'tab2') {
        this.getStatisticsData()
      } else {
        this.getSlowSqlLogData();
      }
    },
    async getvariables() {
      try {
        if (this.requestIng) return;
        this.requestIng = true;

        let res = await sendSQLReq(`show cluster variables`)
        let arr = res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
        
        this.data = arr.reduce((data, item) => {
          const value = item.name == 'slowLogScope' ? item.value.split('|') : item.value ?? '';
          data[item.name] = value;
          return data;
        }, {});

        console.log('res',arr,this.data);
        this.requestIng = false;
      } catch (error) {
        console.log("err");
      }
    },
    async getSlowSqlLogData() {
      try {
        if (this.requestIng) return;
        this.requestIng = true;
        this.slowSqlLogList = [];

        [this.slowSqlLogList, this.total] = await getSlowSqlLogs({
          currentPage: this.currentPage,
          pageSize: this.pageSize,
          conditions: this.conditions,
          deDuplication: this.de_duplication,
          sortBy: this.query_time_sort
        });
        this.requestIng = false;
      } catch (error) {
        console.log("err",error);
      }
    },
    async getStatisticsData() {
      try {
        if (this.requestIng) return;
        this.requestIng = true;
        this.statisticsList = [];

        [this.statisticsList, this.totalTwo] = await getSlowSqlStatistics({
          currentPage: this.currentPageTwo,
          conditions: this.conditions_two,
          pageSize: this.pageSizeTwo,
          orderSql: this.orderSql || ''
        });
        this.requestIng = false;
      } catch (error) {
        console.log("err",error);
      }
    },
    async getAllSlowSqlData() {
      const dataSql = `SELECT
        ${this.de_duplication ? 'LAST_ROW(start_ts) as start_ts,' : 'start_ts,'}
        db, ip, \`user\`, sql, query_time, rows_num FROM log.taos_slow_sql_detail 
        ${this.conditions ? 'WHERE' + this.conditions : ''}
        ${this.de_duplication ? 'PARTITION by sql,db' : ''}
        ORDER BY start_ts DESC
      `
      const countSql = `select count(*) from (${dataSql})`
      let countRes = await sendSQLReq(countSql)
       
      let pageSize = countRes?.code == 0 ? countRes.data[0][0] : 0;

      let res = await sendSQLReq(dataSql);
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
            return [item[0], ""];
          })
        );
      }
    },
    async exportFile() {
      let list = await this.getAllSlowSqlData();
      if (Array.isArray(list)) {
        list.map(item => {
          item.query_time = this.numToFixed(item.query_time)
        })
      }
      const FileName = "slowSql.csv";
      const data = parse(list);
      const blob = new Blob(["\uFEFF" + data], {
        type: "text/csv;charset=utf-8;",
      });
      FileSaver.saveAs(blob, FileName);
    },
    numToFixed(num){
      if (!num) return num;
      return (Number(num)/1000).toFixed(1)
    },
    customSort({column, prop, order}) {
      let sortBy = order ? (order == "descending" ? "DESC" : "ASC") : order
      if (prop == "query_time") {
        this.query_time_sort = sortBy;
        this.getSlowSqlLogData();
      }
      if (prop == "max_query_time" || prop == "avg_query_time") {
        this.orderSql = `${sortBy ? `ORDER BY ${prop} ${sortBy}` : ''}`
        this.getStatisticsData()
      }
     },
     checkQueryTime(_, value, callback) {
      const { query_time_1, query_time_2} = this.filterParams
      if (query_time_1 && query_time_2 && query_time_2 < query_time_1 ) {
        return callback(new Error(this.$t('slowSql.queryTimeTip')));
      } else {
        callback()
      }
    },
  },
  watch: {
    async activeName(val) {
      if (val == "slowSql") {
        await this.getSlowSqlLogData();
        await this.getStatisticsData();
      }
    },
  },
  async created() {
    await this.getSlowSqlLogData();
    await this.getStatisticsData();
    // let result = getDataConfig(this.$i18n.locale);
    // this.configData = result
    // this.getvariables()
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
.flex {
  display: flex;
}
:deep {
  .el-input-number__increase,
  .el-input-number__decrease {
    height: 30px;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  // .el-form-item__content {
  //   display: flex;
  //   justify-content: space-between;
  // }
  // .el-form--inline .el-form-item {
  //   display: inline-flex;
  // }
}
.before {
  width: 200px;
}
.end {
 width: 80px;
}

.ds-select {
  width: 90%;
}
.my-popper {
  max-width: 600px;
  max-height: 600px;
  overflow-y: auto;
  overflow-x: hidden;
}
</style>
