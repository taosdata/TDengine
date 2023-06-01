<template>
  <div>
    <p class="title">
      <span>{{ $t("dataIn.dataSources") }}</span>
    </p>
    <div class="data-source">
      <div class="flexEnd">
        <el-button
          plain
          @click="dialog = true"
          size="small"
          icon="el-icon-plus"
          >{{ $t("datasource.addsource") }}</el-button
        >
      </div>
      <el-table
        style="margin-top: 20px"
        :data="topicList"
        size="mini"
        max-height="250"
      >
        <el-table-column
          :label="$t('datasource.name')"
          prop="localname"
        ></el-table-column>
        <el-table-column
          :label="$t('datasource.type')"
          prop="localtype"
        ></el-table-column>
        <el-table-column
          :label="$t('datasource.target')"
          prop="target"
        ></el-table-column>
        <el-table-column
          :label="$t('datasource.createat')"
          prop="created_at"
        ></el-table-column>
        <el-table-column
          :label="$t('datasource.via')"
          prop="via"
        ></el-table-column>
        <!-- <el-table-column label="Finished At" prop="finished_at"></el-table-column> -->

        <el-table-column :label="$t('datasource.status')" prop="status">
          <template slot-scope="scope">
            <div class="status-operation">
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
                <div slot="content" v-html="scope.row.reason"></div>
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
                  :content="$t('datasource.excutestart')"
                >
                  <el-button
                    plain
                    size="small"
                    @click="start(scope.row)"
                    icon="el-icon-qidong"
                  ></el-button>
                </el-tooltip>
              </template>
              <template v-else>
                <el-tooltip
                  placement="bottom"
                  effect="light"
                  :content="$t('datasource.excutestop')"
                >
                  <el-button
                    plain
                    size="small"
                    @click="stop(scope.row)"
                    icon="el-icon-tingzhi"
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
          width="100"
          class="action"
        >
          <template slot-scope="scope">
            <el-button
              type="primay"
              size="small"
              :disabled="
                scope.row.from_detail === undefined ||
                scope.row.status.toLowerCase() == 'running' ||
                !getEditStatus(scope.row.labels)
              "
              @click="edit(scope.row)"
              icon="el-icon-edit"
            ></el-button>
            <el-button
              plain
              size="small"
              @click="del(scope.row)"
              icon="el-icon-delete"
            ></el-button>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="dialog">
        <AddDialog
          :typeList="typeList"
          @closeDialog="closeDialog"
          @addAgent="addAgent"
          @showMqttDialog="showMqttDialog"
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
    <div v-if="mqttdialog">
      <MqttParserDialog @closeMqttDialog="closeMqttDialog"></MqttParserDialog>
    </div>
  </div>
</template>
<script>
import { Message } from "element-ui";
import { getDatain } from "@/api/explorer/datain";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import AddDialog from "../components/addDialog.vue";
import MqttParserDialog from "../components/mqttConnector.vue";
import Agents from "../components/agents.vue";
import { deepClone } from "@/utils";
export default {
  name: "DataSource",
  components: { AddDialog, Agents, MqttParserDialog },
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
    };
  },
  methods: {
    closeMqttDialog() {
      this.mqttdialog = false;
    },
    showMqttDialog() {
      this.mqttdialog = true;
    },
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
        await excuteDel(data.id)
          .then(() => {
            Message({
              type: "success",
              message: this.$t("datasource.deleteok"),
            });
            this.refresh();
          })
          .catch((err) => {
            return Promise.reject(err);
          });
      });
    },
    edit(data) {
      if (data.from_detail) {
        let editDdata = [].concat(data.from_detail);
        if (data.from_expand && data.from_expand.id == "mqtt") {
          this.$store.commit("app/SET_MQTT_PARSER", data.parser);
          this.$parent.parserobj = deepClone(data.parser);
        }
        let dbname =
          data.to_expand && data.to_expand.subject
            ? data.to_expand.subject
            : "";
        this.$parent.uidata = editDdata;
        localStorage.setItem("datainName", data.name);
        this.$parent.toggleComponent("", data.from_detail.id, data.id, dbname);
      }

      // this.$router.push({
      //   path: `/dataIn/source/${data.data_source_name}`
      // });
    },

    async getList() {
      try {
        this.topicList = [];
        let id = localStorage.getItem("local_clusterID");
        await getDatain(id).then((res) => {
          if (res) {
            this.topicList = res.map((item) => {
              item["localname"] = item.name ? item.name : "tmq+" + item.id;
              item["localtype"] = item.from_detail ? item.from_detail.name : "";
              item["target"] = item.to_expand ? item.to_expand.subject : "";
              item["created_at"] = item.created_at
                ? item.created_at.replace(/(?<=\.)\S+$/, "").replace(".", "") +
                  "Z"
                : "";
              return item;
            });
          }
        });
      } catch (err) {
        return Promise.reject(err);
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
          await excuteStart(data.id).then((res) => {
            this.refresh();
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
          await excuteStop(data.id).then((res) => {
            this.refresh();
          });
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    refresh() {
      this.getList();
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
  },
  mounted() {
    if (this.$parent.$parent.$parent.currentName == "datasource") {
      this.refresh();
    }
    this.typeList = this.sourceList;
  },
};
</script>
<style lang='scss'>
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
  padding: 8px 16px;
}
.flexEnd {
  position: absolute;
  top: 15px;
  z-index: 9999;
  right: 10px;
  .el-button {
    border: none;
    background: transparent;
  }
}
</style>
