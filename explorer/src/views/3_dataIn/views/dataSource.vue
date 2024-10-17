<template>
  <div v-loading="requestIng" style="height: 100%">
    <div class="title">
      <span>{{ $t("dataIn.dataSources") }}</span>
      <div class="flexEnd">
        <el-tooltip
          :content="$t('dataIn.batchOperateTip',[`${$t('dataIn.start')}`])"
          :disabled="!isDisabled"
          placement="top-start"
          effect="light"
          :open-delay="0"
        >
          <el-button
            @click="handleBatchTask('start')"
            plain
            type="primary"
            size="small"
            icon="el-icon-qidong"
            :disabled="isDisabled || $COMMUNITY"
            >{{startCase( $t('dataIn.start')+$t('dataIn.task'))}}</el-button
          >
        </el-tooltip>
        <el-tooltip
          :content="$t('dataIn.batchOperateTip',[`${$t('dataIn.stop')}`])"
          :disabled="!isDisabled"
          placement="top-start"
          effect="light"
          :open-delay="0"
        >
          <el-button
            @click="handleBatchTask('stop')"
            plain
            type="primary"
            size="small"
            icon="el-icon-tingzhi"
            :disabled="isDisabled || $COMMUNITY"
            >{{startCase($t('dataIn.stop')+$t('dataIn.task'))}}</el-button
          >
        </el-tooltip>
        <el-tooltip
          :content="$t('dataIn.batchOperateTip',[`${$t('dataIn.delete')}`])"
          :disabled="!isDisabled"
          placement="top-start"
          effect="light"
          :open-delay="0"
        >
          <el-button
            @click="handleBatchTask('delete')"
            plain
            type="primary"
            size="small"
            icon="el-icon-delete"
            :disabled="isDisabled || $COMMUNITY"
            >{{startCase($t('dataIn.delete')+$t('dataIn.task'))}}</el-button
          >
        </el-tooltip>
        <el-button
          @click="refresh"
          plain
          type="primary"
          size="small"
          icon="el-icon-refresh"
          :disabled="requestIng || $COMMUNITY"
          >{{ $t("refresh") }}</el-button
        >
        <el-button
          @click="addDbSource"
          size="small"
          icon="el-icon-plus"
          plain
          type="primary"
          >{{ $t("datasource.addsource") }}</el-button
        >
      </div>
    </div>
    <div class="data-source">
      <el-table
        ref="dataSourceTable"
        style="margin-top: 20px"
        :data="topicList"
        size="mini"
        :max-height="maxHeight"
        row-key="id"
        :expand-row-keys="expandRowKeys"
        @expand-change="expandChange"
        @selection-change="handleSelectionChange"
        @cell-click="clickAgent"
        @sort-change="handleSortChange"
      >
        <el-table-column type="selection" :reserve-selection="true" width="50"> </el-table-column>
        <el-table-column type="expand">
          <template slot-scope="props">
            <div>
              <el-table
                :data="props.row.taskActivities"
                size="mini"
                class="tabel-expand"
                row-key="at"
              >
                <el-table-column
                  prop="level"
                  :label="$t('dataIn.level')"
                  width="100"
                >
                  <span
                    slot-scope="scope"
                    :style="getLevelStyle(scope.row.level)"
                  >
                    <i
                      class="el-icon-warning"
                      v-if="scope.row.level == 'warn'"
                    ></i>
                    <i
                      class="el-icon-error"
                      v-if="scope.row.level == 'error'"
                    ></i>
                    <i
                      class="el-icon-info"
                      v-if="scope.row.level == 'info'"
                    ></i>
                    {{ scope.row.level }}
                  </span>
                </el-table-column>
                <el-table-column prop="at" :label="$t('dataIn.at')" width="220">
                  <span slot-scope="scope">{{
                    parsinginZone(scope.row.at)
                  }}</span>
                </el-table-column>
                <el-table-column prop="activity" :label="$t('dataIn.activity')">
                  <template slot-scope="scope">
                    <el-tooltip
                      :content="scope.row.activity"
                      placement="top-start"
                    >
                      <span class="nowrap">{{ scope.row.activity }}</span>
                    </el-tooltip>
                  </template>
                </el-table-column>
                <el-table-column
                  prop="context"
                  :label="$t('dataIn.context')"
                ></el-table-column>
              </el-table>
            </div>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.taskid')"
          prop="taskid"
          width="80"
        >
          <template slot-scope="scope">
            <span>
              <i
                class="el-circle"
                style="background-color: #e6a23c"
                v-if="
                  scope.row.taskActivities &&
                  scope.row.taskActivities[0]?.level == 'warn'
                "
              ></i>
              <i
                :class="['el-circle', 'err-circle']"
                style="background-color: #fe6c6c"
                v-else-if="
                  scope.row.taskActivities &&
                  scope.row.taskActivities[0]?.level == 'error'
                "
              ></i>
              <i class="el-circle" style="background-color: #67c23a" v-else></i>
            </span>
            <span style="padding-left: 5px">{{ scope.row.taskid }}</span>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.name2')"
          prop="localname"
          sortable
          :sort-method="getSortMethod('localname')"
          min-width="100"
        >
          <template slot-scope="scope">
            <el-tooltip :content="scope.row.localname" placement="top-start">
              <span class="nowrap">{{ scope.row.localname }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.type')"
          prop="localtype"
          width="180"
          sortable
          :sort-method="getSortMethod('localtype')"
          :filters="dataSourceFilters"
          :filter-method="filterHandler"
        >
          <template slot-scope="scope">
            <el-tooltip :content="dataSourceMap[scope.row.from_expand.id]" placement="top-start">
              <span class="nowrap">{{  dataSourceMap[scope.row.from_expand.id] }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.target')"
          prop="target"
          min-width="100"
        >
          <template slot-scope="scope">
            <el-tooltip :content="scope.row.target" placement="top-start">
              <span class="nowrap">{{ scope.row.target }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.createat')"
          prop="created_at"
          width="220"
        >
          <span slot-scope="scope">{{
            parsinginZone(scope.row.created_at)
          }}</span>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.via')"
          prop="via"
          min-width="100"
        >
          <template slot-scope="{ row }">
            <el-tooltip :content="agentMap[row.via]" placement="top-start">
              <span class="nowrap" style="cursor: pointer">{{
                agentMap[row.via]
              }}</span>
            </el-tooltip>
          </template>
        </el-table-column>

        <el-table-column
          :label="$t('dataIn.metrics')"
          prop="finished_at"
          width="120"
        >
          <template slot-scope="scope">
            <el-button
              @click="checkMetrics(scope.row, scope.row.status.toLowerCase())"
              size="mini"
              style="font-size: 12px; color: #4d6992"
              :disabled="
                scope.row.status.toLowerCase() == 'cancelled' || $COMMUNITY
              "
              >{{ $t("view") }}</el-button
            >
          </template>
        </el-table-column>

        <el-table-column
          :label="$t('datasource.status')"
          prop="status"
          min-width="170"
          sortable
          :sort-method="getSortMethod('status')"
          :filters="statusFilters"
          :filter-method="filterHandler"
        >
          <template slot-scope="scope">
            <div
              class="status-operation"
              style="display: flex; white-space: nowrap"
            >
              <el-tooltip
                v-if="showErrStatus.includes(scope.row.status.toLowerCase())"
                placement="bottom"
                effect="light"
                popper-class="datain"
              >
                <div v-html="scope.row.last_modified_at" slot="content"></div>
                <div
                  slot="content"
                  v-html="scope.row.reason"
                  style="max-height: 200px; overflow: auto"
                ></div>
                <span style="width: 80px; display: inline-block">{{
                    textOfstatus(scope.row.status)
                }}</span>
              </el-tooltip>
              <span style="width: 80px; display: inline-block" v-else>{{
                textOfstatus(scope.row.status)
              }}</span>
              <template
                v-if="
                  permitStartStatus.includes(scope.row.status.toLowerCase())
                "
              >
                <el-tooltip
                  placement="bottom"
                  effect="light"
                  :content="
                    $t('datasource.excutestart').replace(
                      '{name}',
                      scope.row.name
                    )
                  "
                >
                  <el-button
                    plain
                    size="mini"
                    @click="start(scope.row)"
                    icon="el-icon-qidong"
                    :disabled="$COMMUNITY"
                  ></el-button>
                </el-tooltip>
              </template>
              <template
                v-if="permitStopStatus.includes(scope.row.status.toLowerCase())"
              >
                <el-tooltip
                  placement="bottom"
                  effect="light"
                  :content="
                    $t('datasource.excutestop').replace(
                      '{name}',
                      scope.row.name
                    )
                  "
                >
                  <el-button
                    plain
                    size="mini"
                    @click="stop(scope.row)"
                    icon="el-icon-tingzhi"
                    :disabled="$COMMUNITY"
                  ></el-button
                ></el-tooltip>
              </template>
              <template>
                <el-tooltip
                  placement="bottom"
                  effect="light"
                  :content="$t('refresh')"
                >
                  <el-button
                    plain
                    size="mini"
                    @click="refreshCurrentTask(scope.row)"
                    icon="el-icon-refresh"
                    :disabled="$COMMUNITY"
                  ></el-button
                ></el-tooltip>
              </template>
            </div>
            <!-- <template v-if="['stopped','finished','failed'].includes(scope.row.status.toLowerCase())">
              <div class="finished-time">{{scope.row.last_modified_at}}</div>
              <div class="reason">{{scope.row.reason}}</div>
            </template> -->
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('datasource.operation')"
          width="190"
          class="action"
          fixed="right"
        >
          <template slot-scope="scope">
            <el-tooltip
              placement="bottom"
              effect="light"
              :content="
                $t('datasource.viewconfig').replace('{name}', scope.row.name)
              "
            >
              <el-button
                type="primay"
                size="mini"
                :disabled="
                  $COMMUNITY
                    ? $COMMUNITY
                    : scope.row.from_detail === undefined ||
                      !getEditStatus(scope.row.labels)
                "
                @click="view(scope.row, scope.row.status.toLowerCase())"
                icon="el-icon-view"
              ></el-button>
            </el-tooltip>
            <el-tooltip
              placement="bottom"
              effect="light"
              :content="$t('datasource.editconfig')"
            >
              <el-button
                type="primay"
                size="mini"
                :disabled="
                  $COMMUNITY
                    ? $COMMUNITY
                    : scope.row.from_detail === undefined ||
                      !getEditStatus(scope.row.labels)
                "
                @click="edit(scope.row, scope.row.status.toLowerCase())"
                icon="el-icon-edit"
              ></el-button>
            </el-tooltip>
            <el-tooltip
              placement="bottom"
              effect="light"
              :content="$t('delete')"
            >
              <el-button
                plain
                size="mini"
                @click="del(scope.row)"
                icon="el-icon-delete"
                :disabled="$COMMUNITY"
              ></el-button>
            </el-tooltip>
            <el-tooltip
              placement="bottom"
              effect="light"
              :content="$t('clone')"
            >
              <el-button
                plain
                size="mini"
                @click="copyTask(scope.row, scope.row.status.toLowerCase())"
                icon="el-icon-copy-document"
                :disabled="$COMMUNITY"
              ></el-button>
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
    <el-alert
      v-if="$COMMUNITY"
      class="my-alert"
      style="margin-top: 8px"
      type="warning"
      :description="$t('communityDemoDataTip')"
      :closable="true"
      center
    />
  </div>
</template>
<script>
import { Message } from "element-ui";
import {
  getTask,
  refreshTask,
  getTaskActivities,
  getMetrics,
  getMetricsDesc,
  batchStartTask,
  batchStopTask,
  batchDelTask,
} from "@/api/explorer/datain";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import Metrics from "../components/metrics.vue";
import { deepClone, parsinginZone, sort } from "@/utils";
import { getDataSources } from "@/api/explorer/community";
import { dataInMockData } from "@/const";
import _ from 'lodash';

export default {
  name: "DataSource",
  components: {},
  props: {
    sourceList: {
      type: Array,
      default() {
        return [];
      },
    },
    tagName: {
      type: String,
      default: "datasource",
    },
  },

  data() {
    return {
      startCase: _.startCase,
      disable: true,
      typeList: [],
      mqttdialog: false,
      dbsource: null,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      topicList: [],
      dataSourceFilters: [],
      dataSourceMap: {},
      statusFilters: [],
      requestIng: false,
      parsinginZone,
      taskActivities: [],
      expandRowKeys: [],
      metricDisable: false,
      maxHeight: 500,
      // 不允许 start/stop 的状态 sopping, suspending
      permitStartStatus: ['created','failed','stopped','suspended','completed'],
      permitStopStatus: ['queued','running','interrupted','waiting','resumed'],
      showErrStatus: ['waiting','suspending','suspended','failed','interrupted'],
      permitDeleteStatus: ['completed','stopped',' failed', 'interrupted', 'ticked'],
      multipleSelection: [],
    };
  },
  computed: {
    filterMap() {
      return {
        type: this.typeList.map((item) => ({
          text: item.name,
          value: item.name,
        })),
      };
    },
    agentMap() {
      return this.$store.state.app.agentLists.reduce((pre, cur) => {
        pre[cur.id] = cur.name;
        return pre;
      }, {});
    },
    isDisabled() {
      return this.multipleSelection.length < 1;
    },
  },
  watch: {
    "$i18n.locale": {
      deep: true,
      async handler(val) {
        this.statusFilters.forEach((item) => {
          item.text = this.textOfstatus(item.value);
        });
      },
    },
  },
  methods: {
    handlePageChange() {},
    //非root用户不能修改root下创建的数据源
    getEditStatus(data) {
      if (data) {
        let result = data
          .filter((item) => item.includes("user"))
          .toString()
          .split("::");
        if (result[1] == localStorage.getItem("username")) {
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    },
    del(data) {
      this.$confirm(
        this.$t("datasource.deletetip") + data.name + "?",
        this.$t("datasource.warning"),
        {
          confirmButtonText: this.$t("datasource.ok"),
          cancelButtonText: this.$t("datasource.cancel"),
          type: "warning",
        }
      ).then(async () => {
        await this.handleClearInterval();
        let result = await excuteDel(data.id);
        if (result?.message) {
          this.handleSetInterval();
          Message.warning(result.message);
          return;
        }
        Message({
          type: "success",
          message: this.$t("datasource.deleteok"),
        });
        await this.refresh();
        await this.$nextTick(() => {
          this.handleSetInterval();
        });
      });
    },
    view(data, status) {
      this.$parent.isViewable = true;
      this.$parent.sourceName = data.name;
      this.$parent.currentTaskStatus = status;
      this.$parent.agentID = data?.via;
      this.$parent.setEditID(data.id);
      if (data.from_expand) {
        this.$store.commit("app/SET_CURRENT_DBTYPE", data.from_expand?.id);
        this.$store.commit("app/SET_CURRENT_RESUME", data.trigger?.resume);
        this.$store.commit("app/SET_CURRENT_DBNAME", data.target);
        this.$store.commit("app/SET_CURRENT_AGENT", data?.via);
        this.$store.commit("app/SET_CURRENT_DSNAME", data.name);
        // let editDdata = deepClone([].concat(data.from_detail));
        if (
          data.from_expand.id == "mqtt" ||
          data.from_expand.id == "kafka" ||
          data.from_expand.id == "csv" ||
          data.from_expand.id == "mongodb"
        ) {
          this.$store.commit("app/SET_TRANSFORM_PARSERDATA", data.parser);
        }
        if (data.from_expand && data.from_expand.id == "mqtt") {
          let dnsarr = data.from.split("?")[1].split("&");
          let caindex = dnsarr.findIndex((item) => item.includes("ca="));
          let certindex = dnsarr.findIndex((item) => item.includes("cert="));
          let certkeyindex = dnsarr.findIndex((item) =>
            item.includes("cert_key=")
          );
          if (caindex > -1) {
            let file = dnsarr[caindex].split("=")[1].replace("@", "");
            this.$store.commit("app/SET_MQTT_CAFILE", [].concat(file));
          }
          if (certindex > -1) {
            let file = dnsarr[certindex].split("=")[1].replace("@", "");
            this.$store.commit("app/SET_MQTT_CERTFILE", [].concat(file));
          }
          if (certkeyindex > -1) {
            let file = dnsarr[certkeyindex].split("=")[1].replace("@", "");
            this.$store.commit("app/SET_MQTT_CERTKEYFILE", [].concat(file));
          }
          this.$store.commit("app/SET_MQTT_PARSER", data.parser);
          this.$parent.parserobj = deepClone(data.parser);
        }
        if (this.$store.state.app.supportSQL) {
          this.$store.commit("app/SET_HISTORIAN_ECHODATA", data.parser);
          this.$store.commit(
            "app/SET_HISTORIAN_DSN",
            "://" + data.from.split("://")[1]
          );
        }
        // if (data.from_expand && data.from_expand.id == "kafka") {
        //   let payload = deepClone(data.parser.parse.value);
        //   let parser = {
        //     ...data.parser,
        //     parse: {
        //       payload,
        //     },
        //   };
        //   this.$store.commit("app/SET_MQTT_PARSER", parser);
        //   this.$parent.parserobj = deepClone(parser);
        // }
        if (
          data.from_expand &&
          (data.from_expand.id == "opcua" || data.from_expand.id == "opcda")
        ) {
          let dnsarr = data.from.split("?")[1].split("&");
          let fileindex = dnsarr.findIndex((item) =>
            item.includes("csv_config_file=")
          );
          if (fileindex > -1) {
            let file = dnsarr
              .filter((item) => item.includes("csv_config_file="))[0]
              .split("=")[1]
              .replace("@", "");
            // editDdata[0].datasets.value = "csv_config_file";
            this.$store.commit("app/SET_OPC_UANODES", [].concat(file));
          } else {
            // editDdata[0].datasets.value = "select_all_points";
          }

          let certfile = dnsarr
            .filter((item) => item.includes("certificate="))[0]
            ?.split("=")[1]
            .replace("@", "");
          let privatefile = dnsarr
            .filter((item) => item.includes("private_key="))[0]
            ?.split("=")[1]
            .replace("@", "");

          this.$store.commit("app/SET_OPC_CERTFILES", [].concat(certfile));
          this.$store.commit(
            "app/SET_OPC_PRIVATEFILES",
            [].concat(privatefile)
          );
        }

        if (data.from_expand && data.from_expand.id == "csv") {
          this.$store.commit("app/SET_CSV_PARSER", data.parser);

          this.$parent.echoData = deepClone([].concat(data.parser));
          let filelist = data.from.match(/(?<=csv:).*?(?=\?)/)[0];
          let hasheader = data.from.match(/has_header=([^&]*)/)[1];
          let localCols = data.from.match(/(?<=header=).*/)[0];
          if (localCols && localCols.includes("=")) {
            this.$store.commit(
              "app/SET_CSV_LOCAL_COLS",
              localCols.split("=")[1].split(",")
            );
          }
          this.$store.commit("app/SET_CSV_HASHEADER", hasheader);
          this.$store.commit("app/SET_CSV_FILES", filelist);
        }
        let dbname =
          data.to_expand && data.to_expand.subject
            ? data.to_expand.subject
            : "";
        // this.$emit("setEditData", editDdata);
        // this.$set(this.$parent.uidata,0,editDdata)
        // this.$parent.uidata = editDdata;
        localStorage.setItem("datainName", data.name);
        this.$parent.toggleComponent("", data.from_expand.id, data.id, dbname);
      }
    },
    edit(data, status, iscopy) {
      this.$parent.sourceName = data.name;
      this.$parent.currentTaskStatus = status;
      this.$parent.agentID = data?.via;
      this.$parent.setEditID(data.id);
      this.$parent.isCopyable = iscopy;
      this.$parent.isViewable = false;
      this.$store.commit("app/SET_CURRENT_EDITID", data.id);
      if (data.from_expand) {
        this.$store.commit("app/SET_CURRENT_DBTYPE", data.from_expand?.id);
        this.$store.commit("app/SET_CURRENT_RESUME", data.trigger?.resume);
        this.$store.commit("app/SET_CURRENT_DBNAME", data.target);
        this.$store.commit("app/SET_CURRENT_AGENT", data?.via);
        this.$store.commit("app/SET_CURRENT_DSNAME", data.name);
        // let editDdata = deepClone([].concat(data.from_detail));
        if (
          data.from_expand.id == "mqtt" ||
          data.from_expand.id == "kafka" ||
          data.from_expand.id == "csv" ||
          data.from_expand.id == "mongodb"
        ) {
          this.$store.commit("app/SET_TRANSFORM_PARSERDATA", data.parser);
        }
        if (data.from_expand && data.from_expand.id == "mqtt") {
          let dnsarr = data.from.split("?")[1].split("&");
          let caindex = dnsarr.findIndex((item) => item.includes("ca="));
          let certindex = dnsarr.findIndex((item) => item.includes("cert="));
          let certkeyindex = dnsarr.findIndex((item) =>
            item.includes("cert_key=")
          );
          if (caindex > -1) {
            let file = dnsarr[caindex].split("=")[1].replace("@", "");
            this.$store.commit("app/SET_MQTT_CAFILE", [].concat(file));
          }
          if (certindex > -1) {
            let file = dnsarr[certindex].split("=")[1].replace("@", "");
            this.$store.commit("app/SET_MQTT_CERTFILE", [].concat(file));
          }
          if (certkeyindex > -1) {
            let file = dnsarr[certkeyindex].split("=")[1].replace("@", "");
            this.$store.commit("app/SET_MQTT_CERTKEYFILE", [].concat(file));
          }
          this.$store.commit("app/SET_MQTT_PARSER", data.parser);
          this.$parent.parserobj = deepClone(data.parser);
        }
        if (this.$store.state.app.supportSQL) {
          this.$store.commit("app/SET_HISTORIAN_ECHODATA", data.parser);
          this.$store.commit(
            "app/SET_HISTORIAN_DSN",
            "://" + data.from.split("://")[1]
          );
        }
        // if (data.from_expand && data.from_expand.id == "kafka") {
        //   let payload = deepClone(data.parser.parse.value);
        //   let parser = {
        //     ...data.parser,
        //     parse: {
        //       payload,
        //     },
        //   };
        //   this.$store.commit("app/SET_MQTT_PARSER", parser);
        //   this.$parent.parserobj = deepClone(parser);
        // }
        if (
          data.from_expand &&
          (data.from_expand.id == "opcua" || data.from_expand.id == "opcda")
        ) {
          let dnsarr = data.from.split("?")[1].split("&");
          let fileindex = dnsarr.findIndex((item) =>
            item.includes("csv_config_file=")
          );
          if (fileindex > -1) {
            let file = dnsarr
              .filter((item) => item.includes("csv_config_file="))[0]
              .split("=")[1]
              .replace("@", "");
            // editDdata[0].datasets.value = "csv_config_file";
            this.$store.commit("app/SET_OPC_UANODES", [].concat(file));
          } else {
            // editDdata[0].datasets.value = "select_all_points";
          }

          let certfile = dnsarr
            .filter((item) => item.includes("certificate="))[0]
            ?.split("=")[1]
            .replace("@", "");
          let privatefile = dnsarr
            .filter((item) => item.includes("private_key="))[0]
            ?.split("=")[1]
            .replace("@", "");

          this.$store.commit("app/SET_OPC_CERTFILES", [].concat(certfile));
          this.$store.commit(
            "app/SET_OPC_PRIVATEFILES",
            [].concat(privatefile)
          );
        }

        if (data.from_expand && data.from_expand.id == "csv") {
          this.$store.commit("app/SET_CSV_PARSER", data.parser);

          this.$parent.echoData = deepClone([].concat(data.parser));
          let filelist = data.from.match(/(?<=csv:).*?(?=\?)/)[0];
          let hasheader = data.from.match(/has_header=([^&]*)/)[1];
          let localCols = data.from.match(/(?<=header=).*/)[0];
          if (localCols && localCols.includes("=")) {
            this.$store.commit(
              "app/SET_CSV_LOCAL_COLS",
              localCols.split("=")[1].split(",")
            );
          }
          this.$store.commit("app/SET_CSV_HASHEADER", hasheader);
          this.$store.commit("app/SET_CSV_FILES", filelist);
        }
        let dbname =
          data.to_expand && data.to_expand.subject
            ? data.to_expand.subject
            : "";
        // this.$emit("setEditData", editDdata);
        // this.$set(this.$parent.uidata,0,editDdata)
        // this.$parent.uidata = editDdata;
        localStorage.setItem("datainName", data.name);
        this.$parent.toggleComponent(
          "",
          data.from_expand.id,
          data.id,
          dbname,
          iscopy
        );
      }
    },
    //copy一个新的task
    copyTask(data, status) {
      this.$parent.isCopyable = true;
      this.edit(data, status, true);
    },
    addDbSource() {
      this.$store.commit("app/SET_CURRENT_DBNAME", "");
      this.$store.commit("app/SET_CURRENT_AGENT", "");
      this.$store.commit("app/SET_CURRENT_DSNAME", "");
      this.$store.commit("app/SET_CURRENT_DBTYPE", "tmq");
      this.$store.commit("app/SET_CURRENT_EDITID", "");
      this.$parent.currentTaskStatus = "";
      this.$parent.isCopyable = false;
      this.$parent.changeEditable(false);
      this.$parent.toggleComponent("tmq");
    },
    async getList() {
      try {
        this.requestIng = true;
        this.topicList = [];
        let id = localStorage.getItem("local_clusterID");
        let result = await getTask(id, "datain");
        if (result.desc || result.message) {
          this.$error(result.desc || result.message);
          return;
        }
        if (result) {
          this.dataSourceFilters = [];
          const dataSourceFilterSet = {};
          this.statusFilters = [];
          const statusFilterSet = {};
          this.topicList = result.map((item) => {
            if (!dataSourceFilterSet[item.from_expand.id]) {
              this.dataSourceFilters.push({
                value: item.from_expand.id,
                text: this.dataSourceMap[item.from_expand.id],
              });
              dataSourceFilterSet[item.from_expand.id] = true;
            }

            item["statusText"] = this.textOfstatus(item.status);
            if (!statusFilterSet[item.status]) {
              this.statusFilters.push({
                value: item.status,
                text: item.statusText,
              });
              statusFilterSet[item.status] = true;
            }
            
            (item["taskid"] = item.id), (item["localname"] = item.name);
            item["localtype"] = item.from_expand.id;
            item["target"] = item.to_expand ? item.to_expand.subject : "";
            item["created_at"] = item.created_at
              ? item.created_at.replace(/(?<=\.)\S+$/, "").replace(".", "") +
                "Z"
              : "";
            return item;
          });
          this.requestIng = false;
        }
      } catch (err) {
        this.requestIng = false;
        return Promise.reject(err);
      }
    },

    async checkMetrics(data, status) {
      try {
        let result = await getMetrics(data.id);
        if (result.message) {
          this.$error(result.message);
          return;
        }

        if (Object.keys(result).length === 0) {
          switch (status) {
            case "running":
              this.$error(this.$t("datasource.metricTips.running"));
              return;
            case "completed":
              this.$error(this.$t("datasource.metricTips.completed"));
              return;
            case "stopped":
              this.$error(this.$t("datasource.metricTips.stopped"));
              return;
          }
        }
        let metricsDesc = await getMetricsDesc();
        this.$store.commit("SET_DIALOG", {
          component: Metrics,
          params: {
            data: result,
            metricsDesc,
            taskId: data.id,
            type: data.from_expand.id,
          },
          config: {
            title: this.$t("dataIn.metrics"),
            width: "1100px",
          },
          listeners: {
            close: () => {
              this.$store.commit("SET_DIALOG_VISIBLE", false);
            },
          },
        });
      } catch (error) {
        console.log(error);
      }
    },
    start(data, index) {
      try {
        this.$confirm(
          this.$t("datasource.starttip").replace("{dataname}", data.name),
          // `Are you sure to start the ${data.name} task?`,
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(async () => {
          await this.handleClearInterval();
          let result = await excuteStart(data.id);
          if (result && result.message) {
            this.handleSetInterval();
            this.$message({
              dangerouslyUseHTMLString: true,
              message: `<strong>${result.message.replaceAll(
                "\n",
                "<br/>"
              )}</strong>`,
              type: "warning",
            });
            return;
          }
          await this.refresh();
          await this.$nextTick(() => {
            this.handleSetInterval();
          });
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    stop(data) {
      try {
        this.$confirm(
          this.$t("datasource.stoptip").replace("{dataname}", data.name),
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(async () => {
          await this.handleClearInterval();
          let result = await excuteStop(data.id);
          if (result?.message) {
            this.handleSetInterval();
            this.$message({
              dangerouslyUseHTMLString: true,
              message: `<strong>${result.message.replaceAll(
                "\n",
                "<br/>"
              )}</strong>`,
              type: "warning",
            });
            return;
          }
          await this.refresh();
          await this.$nextTick(() => {
            this.handleSetInterval();
          });
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },

    async refresh() {
      await this.getList();
      await this.handleTaskActivities();
      this.$refs.dataSourceTable.clearSelection();
    },
    async refreshCurrentTask(data) {
      try {
        let result = await refreshTask(data.taskid);
        if (result && (result.message || result.desc)) {
          this.$error(result.message || result.desc);
          return;
        }
        let activitList = await this.getCurrentActivities(data.taskid);
        let index = this.topicList.findIndex(
          (item) => item.taskid == data.taskid
        );
        this.topicList.splice(
          index,
          1,
          [].concat(result).map((item) => {
            (item["taskid"] = item.id), (item["localname"] = item.name);
            item["localtype"] = item.from_expand.id;
            item["target"] = item.to_expand ? item.to_expand.subject : "";
            item["created_at"] = item.created_at
              ? item.created_at.replace(/(?<=\.)\S+$/, "").replace(".", "") +
                "Z"
              : "";
            item["taskActivities"] = activitList;
            return item;
          })[0]
        );
        this.refreshCurrentSelection(data.taskid)
        Message.success(this.$t("datasource.refreshsuccess"));
      } catch (error) {
        console.log(error);
      }
    },
    // 先勾选再刷新单独任务的时候更新勾选的数据
    refreshCurrentSelection(taskid) {
      if (this.multipleSelection.length <= 0) return;
      let filterRow = this.topicList.filter(
        (item) => item.taskid == taskid
      );
      this.multipleSelection = this.multipleSelection.map(item => {
        if (item.taskid == taskid) {
          item = {...filterRow[0]}
        }
        return item;
      })
    },
    //显示添加数据源弹窗
    showAddDialog() {},
    async expandChange(row, expandedRows) {
      if (!this.$COMMUNITY) {
        let activitList = await this.getCurrentActivities(row.taskid);
        this.topicList = this.topicList.map((item) => {
          if (item.id == row.taskid) {
            item.taskActivities = deepClone(activitList);
          }
          return item;
        });
      }
    },
    getLevelStyle(level) {
      let style = "";
      switch (level) {
        case "info":
          style = "color: #67c23a";
          break;
        case "warn":
          style = "color: #e6a23c";
          break;
        case "error":
          style = "color: #fe6c6c";
          break;
      }
      return style;
    },
    filterHandler(value, row, column) {
      const property = column["property"];
      return row[property] === value;
    },
    async getCurrentActivities(id) {
      let res = await getTaskActivities(id);
      if (res && res.code && res.code != 0) {
        Message({
          type: "error",
          message: res && res.message,
        });
        return;
      }
      let activitList = res.map((item) => {
        if (item.status == "failed") {
          item.context = item.context?.message;
        }
        if (typeof item.context == "object") {
          item.context = null;
        }
        return item;
      });
      return activitList;
    },
    handleTaskActivities() {
      this.topicList.map(async (task, index) => {
        let res = await getTaskActivities(task.id);
        let activitList = res.map((item) => {
          if (item.status == "failed") {
            item.context = item.context?.message;
          }
          if (typeof item.context == "object") {
            item.context = null;
          }
          return item;
        });
        this.$set(this.topicList, index, {
          ...task,
          taskActivities: activitList,
        });
      });
    },
    textOfstatus(value) {
      return this.$t("statuses." + value);
    },
    handleClearInterval() {
      this.timer && clearInterval(this.timer);
    },
    handleSetInterval() {
      this.timer = setInterval(() => {
        this.handleTaskActivities();
      }, 10000);
    },
    clickAgent(row, column, cell, event) {
      if (column.property === "via" && row.via) {
        this.$store.state.app.activeName = "agent";
        this.$store.state.app.viaId = row.via;
      }
    },
    handleSelectionChange(val) {
      this.multipleSelection = val;
    },
    getSortMethod(prop) {
      let _this = this;
      return function (a, b) {
        let value1 = a[prop];
        let value2 = b[prop];
        if (value2 === undefined || value2 === null) {
          return 1;
        } else if (value1 === undefined || value1 === null) {
          return -1;
        }

        if (_this["textOf" + prop]) {
          value1 = _this["textOf" + prop](value1);
          value2 = _this["textOf" + prop](value2);
        }

        if (value1 > value2) {
          return 1;
        } else if (value1 < value2) {
          return -1;
        } else {
          if (!_this.sortProps) {
            return 0;
          }
          // let reverse = (_this.sortProps[0] === prop &&_this.sortProps[1] === "descending");

          for (let i = 0; i < _this.sortProps.length; i+=2) {
            if (prop === _this.sortProps[i]) {
              continue;
            }
            let thisProp = _this.sortProps[i];
            let thisOrder = _this.sortProps[i+1];
            let va = a[thisProp];
            let vb = b[thisProp];
            if (_this["textOf" + thisProp]) {
              if (va) {
                va = _this["textOf" + thisProp](va);
              }
              if (vb) {
                vb = _this["textOf" + thisProp](vb);
              }
            }
            // if (reverse) {
            //   thisOrder = (thisOrder === "ascending" ? "descending" : "ascending");
            // }

            let r = sort(va, vb, "ascending");
            if (r !== 0) {
              return r;
            }
          }
          return 0;
        }
      };
    },
    handleSortChange({prop, order}) {
      if (!this.sortProps) {
        this.sortProps = [];
      }

      // 取消排序
      if (!order) {
        this.sortProps = [];
        return;
      }

      if (this.sortProps.length === 0) {
        this.sortProps.push(prop);
        this.sortProps.push(order);
        return;
      }

      if (this.sortProps[0] === prop) {
        this.sortProps[1] = order;
        return;
      }

      let newSortProps = [prop, order];
      for (let i = 0; i < this.sortProps.length; i+=2) {
        if (this.sortProps[i] !== prop) {
          newSortProps.push(this.sortProps[i]);
          newSortProps.push(this.sortProps[i+1]);
        }
      }
      this.sortProps = newSortProps;
    },
    filterBatchIds(permitStatus) {
      let result = [];
      this.multipleSelection.filter((item) => {
        if (permitStatus.includes(item.status)) {
          result.push(item.id);
        }
      });
      return result;
    },
    handlerConfirm(content, excuteFn, ids, showConfirmButton) {
      try {
        this.$confirm(
          content,
          this.$t("datasource.warning"),
          {
            confirmButtonText: this.$t("datasource.ok"),
            cancelButtonText: this.$t("datasource.cancel"),
            type: "warning",
            confirmButtonClass: showConfirmButton ? '' : "not-show"
          }
        ).then(async () => {
          await this.handleClearInterval();
          let result = await excuteFn({ids});
          if (result?.message) {
            this.$message({
              dangerouslyUseHTMLString: true,
              message: `
              <strong>
                ${result.message}
              </strong><br/>
              <ul>
                ${result.data.map(item => {
                  return '<li>id:'+ item.id + ' '+ item.error + '</li>'
                }).join('')}
              </ul>`,
              type: "warning",
              duration: 30000,
              showClose: true
            });
          } else {
            this.$message({
              type: 'success',
              message: `${this.$t('operateSucc')}`
            })
          }
          this.$refs.dataSourceTable.clearSelection();
          await this.refresh();
          await this.$nextTick(() => {
            this.handleSetInterval();
          });
        });
      } catch (err){
        return Promise.reject(err);
      }
    },
    async handleBatchTask(type) {
      let ids = [];
      let content, excuteFn = null
      let showConfirmButton = true
      // this.requestIng = true;
      switch (type) {
        case "start":
          ids = this.filterBatchIds(this.permitStartStatus);
          excuteFn = batchStartTask
          content = this.$t('replication.taskStart').replace('{id}',ids)
          break;

        case "stop":
          ids = this.filterBatchIds(this.permitStopStatus);
          excuteFn = batchStopTask;
          content = this.$t('replication.taskStop').replace('{id}',ids)
          break;

        case "delete":
          ids = this.filterBatchIds(this.permitDeleteStatus);
          excuteFn = batchDelTask;
          content = this.$t('replication.backupDel').replace('{id}',ids)
          break;
      }
      if (ids.length < 1) {
        showConfirmButton = false
        content = this.$t('dataIn.noTaskOperateTip',[`${this.$t(`dataIn.${type}`)}`])
      } 
      this.handlerConfirm(content, excuteFn, ids, showConfirmButton)
    },
    //清除transformer相关的存储数据
    clearTransformerStore() {
      this.$store.commit("app/SET_FILTER_PARSE_DATA", null);
      this.$store.commit("app/SET_EXTRACT_PARSE_DATA", null);
      this.$store.commit("app/SET_ECHO_MAP_DATA", null);
      this.$store.commit("app/SET_TRANSFORM_COL_IDENTIFIED", []);
      this.$store.commit("app/SET_TRANSFORM_PARSERDATA", null);
      this.$store.commit("app/SET_TRANSFORMER_MAPCOLUMNS", null);
      this.$store.commit("app/SET_CSV_LOCAL_COLS", []);
      this.$store.commit("app/SET_CSV_TRANSFORMER_PARSER", null);
      this.$store.commit("app/SET_CSV_PARSER", null);
    },
    handleResize() {
      const windowHeight = window.innerHeight;
      this.maxHeight = windowHeight - 300;
    }
  },
  mounted() {
    const ds = getDataSources(this.$i18n.locale);
    ds.forEach((item) => {
      this.dataSourceMap[item.id] = item.name;
    });

    this.clearTransformerStore();
    this.$nextTick(() => {
      this.handleResize();
    })
    window.addEventListener('resize', this.handleResize); 
    if (this.$COMMUNITY) {
      this.topicList = dataInMockData;
    } else {
      if (this.$parent.$parent.$parent.currentName == "datasource") {
        this.refresh().then(() => {
          this.typeList = this.sourceList;
        });
        this.$nextTick(() => {
          this.handleSetInterval();
        });
      }
    }
  },
  beforeDestroy() {
    this.handleClearInterval();
    window.removeEventListener('resize', this.handleResize);
  },
};
</script>
<style lang="scss">
.el-tooltip__popper {
  max-width: 450px !important;
}
.not-show {
  display: none;
}
</style>
<style lang="scss" scoped>
::v-deep.el-form-item__label {
  white-space: nowrap !important;
  margin-right: 100px;
}
.el-form-item {
  display: flex;
}
::v-deep.el-form-item--mini .el-form-item__content {
  margin-left: 0px !important;
}
::v-deep.el-input--mini .el-input__inner,
::v-deep.el-input.el-input--mini.el-input--suffix {
  width: 172px !important;
}
::v-deep.input.el-input__inner {
  width: 172px !important;
}
.title {
  background-color: #ecf8ff;
  border-left-color: #50bfff;
  color: #333;
  border-left-width: 5px;
  border-left-style: solid;
  border-radius: 4px;
  font-size: 16px;
  margin: 10px 0;
  padding: 12px 16px;
  height: 44px;
}
.flexEnd {
  position: absolute;
  top: 16px;
  z-index: 9;
  right: 10px;
  .el-button {
    border: 1px solid transparent;
    background: transparent;
    // color: #4259ce;
    font-size: 14px;
    &:hover {
      // background: #fff;
      color: #4259ce;
      border: 1px solid #4259ce;
    }
    &:focus {
      color: #4259ce;
    }
  }
}

.tabel-expand {
  min-width: 70%;
  margin-left: 40px;
  padding: 0px 5px;
  ::v-deep.el-table th.el-table__cell.is-leaf {
    border: none !important;
  }
  ::v-deep.el-table td.el-table__cell {
    border: none !important;
  }
}

::v-deep.el-table td.el-table__cell div {
  word-wrap: break-word;
  word-break: break-word;
}
.el-circle {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
}
.err-circle {
  animation: circle 1s infinite;
}
.my-alert ::v-deep.el-alert .el-alert__description {
  font-size: 14px;
}
@keyframes circle {
  0% {
    opacity: 1;
  }
  100% {
    opacity: 0;
  }
}
</style>
<style lang="scss">
.db-metrics {
  max-height: 300px;
  li {
    display: flex;
    span {
      display: inline-block;
      flex: 1;
      padding: 3px 10px;
    }
    &:first-child {
      background: #f5f7fa;
      padding: 4px 10px;
      border-top: 1px solid #eaeefb;
    }
    border: 1px solid #eaeefb;
    border-top: none;
  }
}
</style>
