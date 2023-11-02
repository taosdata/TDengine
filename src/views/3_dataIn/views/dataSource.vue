<template>
  <div v-loading="requestIng">
    <div class="title">
      <span>{{ $t("dataIn.dataSources") }}</span>
      <div class="flexEnd">
        <el-button
          @click="refresh"
          size="small"
          icon="el-icon-refresh"
          :disabled="requestIng"
          >{{ $t("refresh") }}</el-button
        >
        <el-button @click="addDbSource" size="small" icon="el-icon-plus">{{
          $t("datasource.addsource")
        }}</el-button>
      </div>
    </div>
    <div class="data-source">
      <el-table
        style="margin-top: 20px"
        :data="topicList"
        size="mini"
        max-height="250"
        row-key="taskid"
        :expand-row-keys="expandRowKeys"
        @expand-change="expandChange"
      >
        <el-table-column type="expand">
          <template>
            <div>
              <el-table
                :data="taskActivities"
                size="mini"
                class="tabel-expand"
                max-height="160"
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
                <el-table-column
                  prop="activity"
                  :label="$t('dataIn.activity')"
                ></el-table-column>
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
          show-overflow-tooltip
        ></el-table-column>
        <el-table-column
          :label="$t('datasource.name2')"
          prop="localname"
          min-width="100"
          show-overflow-tooltip
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
          show-overflow-tooltip
          :filters="filterMap.type"
          :filter-method="filterHandler"
        ></el-table-column>
        <el-table-column
          :label="$t('datasource.target')"
          prop="target"
          min-width="100"
          show-overflow-tooltip
        ></el-table-column>
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
          show-overflow-tooltip
        >
          <template slot-scope="{ row }">
            {{ agentMap[row.via] }}
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
                scope.row.status.toLowerCase() == 'failed' ||
                scope.row.status.toLowerCase() == 'cancelled'
              "
              >{{ $t("view") }}</el-button
            >
          </template>
        </el-table-column>

        <el-table-column
          :label="$t('datasource.status')"
          prop="status"
          min-width="170"
        >
          <template slot-scope="scope">
            <div
              class="status-operation"
              style="display: flex; white-space: nowrap"
            >
              <el-tooltip
                v-if="
                  ['stopped', 'finished', 'failed'].includes(
                    scope.row.status.toLowerCase()
                  )
                "
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
                  scope.row.status
                }}</span>
              </el-tooltip>
              <span style="width: 80px; display: inline-block" v-else>{{
                scope.row.status
              }}</span>
              <template v-if="scope.row.status.toLowerCase() !== 'running'">
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
                  ></el-button>
                </el-tooltip>
              </template>
              <template v-else>
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
          width="150"
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
                  scope.row.from_detail === undefined ||
                  !getEditStatus(scope.row.labels)
                "
                @click="edit(scope.row, scope.row.status.toLowerCase())"
                icon="el-icon-view"
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
              ></el-button>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="dialog">
        <AddDialog
          :typeList="typeList"
          @closeDialog="closeDialog"
          @addAgent="addAgent"
          ref="agentdialog"
        ></AddDialog>
      </div>
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
    <div class="agent" style="margin-top: 20px">
      <Agents ref="agents" />
    </div>
  </div>
</template>
<script>
import { Message, Switch } from "element-ui";
import {
  getTask,
  refreshTask,
  getTaskActivities,
  getMetrics,
} from "@/api/explorer/datain";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import AddDialog from "../components/addDialog.vue";
import Agents from "../components/agents.vue";
import Metrics from "../components/metrics.vue";
import { deepClone, parsinginZone } from "@/utils";
export default {
  name: "DataSource",
  components: { AddDialog, Agents },
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
      disable: true,
      typeList: [],
      mqttdialog: false,
      dbsource: null,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      topicList: [],
      requestIng: false,
      parsinginZone,
      taskActivities: [],
      expandRowKeys: [],
      metricDisable: false,
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
        let result = await excuteDel(data.id);
        if (result?.message) {
          Message.warning(result.message);
          return;
        }
        Message({
          type: "success",
          message: this.$t("datasource.deleteok"),
        });
        this.refresh();
      });
    },
    edit(data, status, iscopy) {
      this.$parent.sourceName = data.name;
      this.$parent.currentTaskStatus = status;
      this.$parent.agentID = data?.via;
      this.$parent.setEditID(data.id);
      this.$parent.isCopyable = iscopy;
      this.$store.commit("app/SET_CURRENT_EDITID", data.id);
      if (data.from_detail) {
        this.$store.commit("app/SET_CURRENT_DBTYPE", data.from_detail?.id);

        this.$store.commit("app/SET_CURRENT_DBNAME", data.target);
        this.$store.commit("app/SET_CURRENT_AGENT", data?.via);
        this.$store.commit("app/SET_CURRENT_DSNAME", data.name);
        let editDdata = deepClone([].concat(data.from_detail));
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
        if (data.from_expand && data.from_expand.id == "kafka") {
          let payload = deepClone(data.parser.parse.value);
          let parser = {
            ...data.parser,
            parse: {
              payload,
            },
          };
          this.$store.commit("app/SET_MQTT_PARSER", parser);
          this.$parent.parserobj = deepClone(parser);
        }
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
            editDdata[0].datasets.value = "csv_config_file";
            this.$store.commit("app/SET_OPC_UANODES", [].concat(file));
          } else {
            editDdata[0].datasets.value = "select_all_points";
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
          let hasheader = data.from.match(/(?<=has_header=).*/)[0];
          this.$store.commit("app/SET_CSV_HASHEADER", hasheader);
          this.$store.commit("app/SET_CSV_FILES", filelist);
        }
        let dbname =
          data.to_expand && data.to_expand.subject
            ? data.to_expand.subject
            : "";
        this.$emit("setEditData", editDdata);
        // this.$set(this.$parent.uidata,0,editDdata)
        // this.$parent.uidata = editDdata;
        localStorage.setItem("datainName", data.name);
        this.$parent.toggleComponent(
          "",
          data.from_detail.id,
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
          Message.error(result.desc || result.message);
          return;
        }
        if (result) {
          this.topicList = result.map((item) => {
            (item["taskid"] = item.id), (item["localname"] = item.name);
            item["localtype"] = item.from_detail ? item.from_detail.name : "";
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
          Message.error(result.message);
          return;
        }
        let array = Object.entries(result).map((item) => ({
          name: item[0],
          value: item[1],
        }));
        if (Array.from(array).length == 0) {
          switch (status) {
            case "running":
              Message.error(this.$t("datasource.metricTips.running"));
              return;
            case "completed":
              Message.error(this.$t("datasource.metricTips.completed"));
              return;
            case "stopped":
              Message.error(this.$t("datasource.metricTips.stopped"));
              return;
          }
        }
        this.$store.commit("SET_DIALOG", {
          component: Metrics,
          params: {
            data: array,
          },
          config: {
            title: this.$t("dataIn.metrics"),
            width: "800px",
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
          let result = await excuteStart(data.id);
          if (result?.message) {
            this.$message({
              dangerouslyUseHTMLString: true,
              message: `<strong>${result.message.replaceAll('\n','<br/>')}</strong>`,
              type: "warning",
            });
            return;
          }
          this.refresh();
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
          let result = await excuteStop(data.id);
          if (result?.message) {
            this.$message({
              dangerouslyUseHTMLString: true,
              message: `<strong>${result.message.replaceAll('\n','<br/>')}</strong>`,
              type: "warning",
            });
            return;
          }
          await this.refresh();
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },

    async refresh() {
      await this.getList();
      await this.$refs.agents?.refresh();
    },
    async refreshCurrentTask(data) {
      try {
        let result = await refreshTask(data.taskid);
        if (result && (result.message || result.desc)) {
          Message.error(result.message || result.desc);
          return;
        }
        let index = this.topicList.findIndex(
          (item) => item.taskid == data.taskid
        );
        this.topicList.splice(
          index,
          1,
          [].concat(result).map((item) => {
            (item["taskid"] = item.id), (item["localname"] = item.name);
            item["localtype"] = item.from_detail ? item.from_detail.name : "";
            item["target"] = item.to_expand ? item.to_expand.subject : "";
            item["created_at"] = item.created_at
              ? item.created_at.replace(/(?<=\.)\S+$/, "").replace(".", "") +
                "Z"
              : "";
            return item;
          })[0]
        );
        Message.success(this.$t("datasource.refreshsuccess"));
      } catch (error) {
        console.log(error);
      }
    },
    //显示添加数据源弹窗
    showAddDialog() {},
    closeDialog() {
      this.dialog = false;
    },
    // 显示添加代理弹框
    addAgent() {
      this.$refs.agents.add();
    },
    async expandChange(row, expandedRows) {
      if (row.taskid == this.expandRowKeys[0]) {
        this.expandRowKeys = [];
        return;
      }
      this.taskActivities = [];
      let res = await getTaskActivities(row.taskid);
      this.expandRowKeys = [row.taskid];
      if (res && res.code && res.code != 0) {
        Message({
          type: "error",
          message: res && res.message,
        });
        return;
      }
      let activitList = res.map((item) => {
        if (item.status == "failed") {
          item.context = item.context.message;
        }
        if (typeof item.context == "object") {
          item.context = null;
        }
        return item;
      });
      this.taskActivities = activitList;
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
  },
  mounted() {
    if (this.$parent.$parent.$parent.currentName == "datasource") {
      this.refresh().then(() => {
        this.typeList = this.sourceList;
      });
    }
  },
};
</script>
<style lang="scss">
.el-tooltip__popper {
  max-width: 450px !important;
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
}
.flexEnd {
  position: absolute;
  top: 16px;
  z-index: 9999;
  right: 10px;
  .el-button {
    border: 1px solid transparent;
    background: transparent;
    color: #4259ce;
    font-size: 14px;
    &:hover {
      background: #fff;
      border: 1px solid #4259ce;
    }
  }
}

.tabel-expand {
  width: 64%;
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
