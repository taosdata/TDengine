<template>
  <div class="data-agent">
    <div class="title">
      <span>{{ $t("topic.agent") }}</span>
      <div class="flexEnd">
        <el-button
        plain
        type="primary"
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng || $COMMUNITY"
        >{{ $t("refresh") }}</el-button
      >
      <el-tooltip
        placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
      >
        <template slot="content">
          <span v-html="$t('communityTip')"></span>
        </template>
        <el-button plain type="primary" @click="add" size="small" icon="el-icon-plus" :disabled="$COMMUNITY">{{
          $t("taosagents.createnewagent")
        }}</el-button>
      </el-tooltip>
      </div>
    </div>

    <el-table
      style="margin-top: 20px"
      :data="agentList"
      size="mini"
      row-key="id"
      :max-height="maxHeight"
      :expand-row-keys="expandRowKeys"
      @expand-change="expandChange"
      highlight-current-row
      ref="singleTable"
    >
      <el-table-column type="expand">
        <template>
          <div>
            <el-table
              :data="agentActivities"
              size="mini"
              class="tabel-expand"
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
                  <i class="el-icon-info" v-if="scope.row.level == 'info'"></i>
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
              >
                <template slot-scope="scope">
                  <el-tooltip :content="scope.row.activity" placement="top-start">
                    <span class="nowrap">{{ scope.row.activity }}</span>
                  </el-tooltip>
                </template> 
              </el-table-column>
              <el-table-column
                prop="context"
                :label="$t('dataIn.context')"
              >
                <template slot-scope="scope">
                  <el-tooltip :content="scope.row.context" placement="top-start">
                    <span class="nowrap">{{ scope.row.context }}</span>
                  </el-tooltip>
                </template> 
              </el-table-column>
            </el-table>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="ID" prop="id"></el-table-column>
      <!-- <el-table-column
        :label="$t('taosagents.cluster_id')"
        prop="cluster_id"
        width="200"
      ></el-table-column> -->
      <el-table-column
        :label="$t('taosagents.name')"
        prop="name"
      ></el-table-column>

      <el-table-column :label="$t('taosagents.created_at')" prop="created_at">
        <span slot-scope="scope">{{
          parsinginZone(scope.row.created_at)
        }}</span>
      </el-table-column>
      <el-table-column
        :label="$t('taosagents.status')"
        prop="status"
      >
        <span slot-scope="scope">{{
          handleDSStatus(scope.row.status)
        }}</span>
      </el-table-column>
      <!-- <el-table-column
        :label="$t('taosagents.dsn')"
        prop="dsn"
        width="200"
      ></el-table-column> -->

      <!-- <el-table-column
        :label="$t('taosagents.last_modified_at')"
        prop="last_modified_at"
        width="250"
      ></el-table-column>

      <el-table-column
        :label="$t('taosagents.status')"
        prop="status"
        width="100"
      >
        <template slot-scope="scope">
          <div class="status-operation">
            <el-tooltip
              v-if="
                scope.row.status &&
                ['stopped', 'finished', 'failed'].includes(
                  scope.row.status.toLowerCase()
                )
              "
              placement="bottom"
              effect="light"
              popper-class="backup"
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
          </div>
        </template>
      </el-table-column> -->
      <!-- <el-table-column
        :label="$t('taosagents.user_id')"
        prop="user_id"
      ></el-table-column> -->
      <el-table-column :label="$t('taosuser.operation')" width="100">
        <template slot-scope="scope">
          <!-- <el-switch
            :value="scope.row.status.toLowerCase() == 'running'"
            active-color="rgb(66, 89, 206)"
            inactive-color="#dcdfe6"
            @change="switchOperation($event, scope.row)"
          >
          </el-switch> -->
          <el-button
            plain
            size="mini"
            @click="edit(scope.row, scope.$index)"
            icon="el-icon-edit"
            :disabled="$COMMUNITY"
          ></el-button>
          <!-- <el-button
            plain
            size="small"
            @click="start(scope.row, scope.$index)"
            icon="el-icon-qidong"
          ></el-button> -->
          <!-- <el-button
            plain
            size="small"
            @click="stop(scope.row, scope.$index)"
            icon="el-icon-tingzhi"
          ></el-button> -->
          <el-button
            plain
            size="mini"
            @click="del(scope.row)"
            icon="el-icon-delete"
            :disabled="$COMMUNITY"
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
    <el-dialog
      align="center"
      :title="dialogTitle"
      width="600px"
      :visible.sync="dialog"
      @close="closeDialog"
      :destroy-on-close="true"
      :close-on-click-modal="false"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="120px"
        class="demo-ruleForm"
      >
        <el-form-item prop="name" :label="$t('taosagents.name')">
          <el-input v-model.trim="ruleForm.name" :maxlength="20"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            @click="submit"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
    <el-dialog
      class="copy-agent"
      align="center"
      :title="$t('copyagent')"
      width="1025px"
      :visible.sync="copyDialog"
      :destroy-on-close="true"
      :before-close="beforeClose"
      :close-on-click-modal="false"
    >
      <AgentDoc :token="agenttoken"></AgentDoc>
      <!-- <el-alert
        :title="$t('copyagentWaring')"
        type="warning"
        :closable="false"
        show-icon>
      </el-alert>
      <div style="display: flex" class="agentcopy">
        <span class="agent-token">{{ agenttoken }}</span>
        <span class="copy-icon" @click="copyToken(agenttoken)">
          <i class="el-icon-copy-document"></i>
          {{ $t("copy") }}
        </span>
      </div> -->
    </el-dialog>
    <el-dialog
      :title="dialogTitle"
      width="620px"
      :visible.sync="showAgent"
      :destroy-on-close="true"
      @close="closeDialog"
      :close-on-click-modal="false"
    >
      <AddAgent :agent="currentRow" :key="showAgent"></AddAgent>
    </el-dialog>
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
import {
  getAgentsData,
  addNewAgent,
  deleteAgent,
  editAgent,
} from "@/api/explorer/agent";
import { copy } from "@/utils/index";
import { getAgentActivities } from "@/api/explorer/datain";
import { getDataSources } from "@/api/explorer/community";
import { Message } from "element-ui";
import { parsinginZone } from "@/utils";
import AgentDoc from "./agentDoc.vue";
import AddAgent from "./addAgent.vue";
import { agentMockData } from "@/const";
export default {
  name: "Agent",
  components: { AgentDoc, AddAgent },
  data() {
    return {
      expireTimeOPtion: {
        disabledDate(time) {
          return time.getTime() < Date.now();
        },
      },
      currentAgent: "",
      showeditAgent: false,
      agenttoken: "",
      showAgent: false,
      requestIng: false,
      dblist: [],
      isEditDialog: false,
      dialogTitle: "",
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      copyDialog: false,
      operateStatus: true,
      currentRow: {},
      clusterid: localStorage.getItem("local_clusterID"),
      ruleForm: {
        name: "",
      },
      rules: {
        name: [
          {
            message: this.$t("taosagents.rules.name"),
            trigger: "blur",
            required: true,
          },
        ],
      },
      agentList: [],

      connectorList: [],
      parsinginZone,
      agentActivities: [],
      expandRowKeys: [],
      maxHeight: 500,
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.name) {
        return true;
      }
      return false;
    },
  },
  methods: {
    closeDialog() {
      this.$store.commit("app/SET_AGENT_DIALOG", false);
    },

    beforeClose() {
      this.$confirm(this.$t("datasource.copytokentip"), this.$t("tips"), {
        confirmButtonText: this.$t("datasource.ok"),
        cancelButtonText: this.$t("datasource.cancel"),
        type: "warning",
        center: true,
      })
        .then(() => {
          this.copyToken(this.agenttoken);
          this.copyDialog = false;
        })
        .catch(() => {
          this.copyDialog = true;
        });
    },
    handlePageChange() {},
    
    del(data) {
      this.$confirm(
        this.$t("taosagents.deletetip").replace(/{id}/, data.id),
        this.$t("warning"),
        {
          confirmButtonText: this.$t("ok"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      ).then(async () => {
        try {
          deleteAgent(data.id)
            .then((res) => {
              res && res.message && this.$error(res.message);
              this.getAgents();
            })
            .catch((err) => {
              err.response.data &&
                err.response.data.message &&
                this.$error(err.response.data.message);
            });
        } catch (err) {
          err.response.data &&
            err.response.data.message &&
            this.$error(err.response.data.message);
        }
      });
    },
    add() {
      this.$set(this, "currentRow", {});
      this.$store.commit("app/SET_AGENT_DIALOG", true);
      this.dialogTitle = this.$t("taosagents.createnewagent");
      this.isEditDialog = false;
      // this.dialog = true;
      this.ruleForm.name = "";
    },
    refresh() {
      this.getAgents();
    },
    edit(data) {
      this.dialogTitle = this.$t("taosagents.edittitle");
      // this.isEditDialog = true;
      // this.dialog = true;
      this.$store.commit("app/SET_AGENT_DIALOG", true);
      this.ruleForm.name = data.name;
      this.currentRow = data;
    },
    copyToken(text) {
      copy(text);
    },
    //切换状态
    switchOperation(val, data) {
      if (val) {
        this.$confirm(
          this.$t("replication.backupTip")
            .replace("{operate}", "start")
            .replace("{id}", data.id),
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(() => {
          this.start(val, data);
        });
      } else {
        this.$confirm(
          this.$t("replication.backupTip")
            .replace("{operate}", "stop")
            .replace("{id}", data.id),
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(() => {
          this.stop(val, data);
        });
      }
    },
    submit() {
      if (this.isEditDialog) {
        //调用编辑接口
        this.editAgentData();
      } else {
        this.addAgentData();
      }
    },
    async editAgentData() {
      try {
        let params = {
          name: this.ruleForm.name,
        };
        let result = await editAgent(this.currentRow.id, params);
        this.dialog = false;
        if (result.message) {
          this.$error(result.message);
          return;
        }
        this.getAgents();
        Message({
          type: "success",
          message: this.$t("operateSucc"),
        });
      } catch (error) {
        Message({
          type: "error",
          message: error || error.message,
        });
        console.log(error);
      }
    },
    async getAgents() {
      try {
        this.requestIng = true;
        this.agentList = (await getAgentsData()).map((item) => {
          item["created_at"] = item.created_at
            ? item.created_at.replace(/(?<=\.)\S+$/, "").replace(".", "") + "Z"
            : "";
          return item;
        });
        this.$store.commit("app/SET_AGENT_LISTS", this.agentList);
        this.requestIng = false;
      } catch (err) {
        this.requestIng = false;
        err.response.data.message && this.$error(err.response.data.message);
      }
    },
    async getConnectorTypes() {
      try {
        let result = getDataSources(this.$i18n.locale);
        this.connectorList =
          result.length > 0
            ? result
                .map((item) => {
                  return {
                    id: item.id,
                    name: item.name,
                  };
                })
                .filter((val) => val.id != "tmq")
            : [];
      } catch (error) {
        console.log(error);
      }
    },
    async addAgentData() {
      try {
        let params = {
          cluster_id: this.clusterid,
          dsn: localStorage.getItem("base_url"),
          name: this.ruleForm.name,
          user_id: localStorage.getItem("username"),
        };
        let result = await addNewAgent(params);
        this.dialog = false;
        if (result.message) {
          this.$error(result.message);
          return;
        }
        await this.getAgents();

        // this.$parent.$refs.agentdialog.agentList = this.agentList.map(
        //   (agent) => {
        //     return {
        //       value: agent.id,
        //       label:
        //         agent.id +
        //         "." +
        //         agent.name +
        //         (new Date(agent.expire_date) < Date.now()
        //           ? "（" + this.$t("datasource.expired") + "）"
        //           : ""),
        //       disabled: new Date(agent.expire_date) < Date.now(),
        //       ...agent,
        //     };
        //   }
        // );
        if (result.token) {
          this.agenttoken = result.token;
          this.copyDialog = true;
        }
      } catch (err) {
        err.response.data.message && this.$error(err.response.data.message);
      }
    },
    async expandChange(row, expandedRows) {
      if (!this.$COMMUNITY) {
        if (row.id == this.expandRowKeys[0]) {
          this.expandRowKeys = [];
          return;
        }
        this.agentActivities = [];
        let res = await getAgentActivities(row.id);
        this.expandRowKeys = [row.id];
        if (res && res.code && res.code != 0) {
          Message({
            type: "error",
            message: res && res.message,
          });
          return;
        }
        this.refresh();
        let activitList = res.map((item) => {
          if (item.status == "failed") {
            item.context = item.context.message;
          }
          if (typeof item.context == "object") {
            item.context = null;
          }
          return item;
        });
        this.agentActivities = activitList;
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
    handleDSStatus(value) {
      return this.$t('statuses.' + value);
    },
    setCurrent(row) {
      this.$refs.singleTable.setCurrentRow(row);
    },
    handleResize() {
      const windowHeight = window.innerHeight;
      this.maxHeight = windowHeight - 300;
    }
  },
  created() {
    if (this.$COMMUNITY) {
      this.agentList = agentMockData;
      this.agentActivities = agentMockData.agentActivities
    } else {
      this.getAgents();
      this.getConnectorTypes();
    }
  },
  mounted() {
    this.$nextTick(() => {
      this.handleResize();
    })
    window.addEventListener('resize', this.handleResize) 
  },
  beforeDestroy() {
    window.removeEventListener('resize', this.handleResize);
  },
  watch: {
    "$store.state.app.agentLists": {
      deep: true,
      handler(val) {
        this.$set(this, "agentList", val);
      },
    },
    "$store.state.app.agentDialog": {
      handler(val) {
        this.showAgent = val;
      },
    },
    "$store.state.app.viaId": {
      handler(via) {
        const row = this.agentList.filter(item => item.id == via)[0]
        this.setCurrent(row)
      }
    },
    "$store.state.app.activeName": {
      handler(active) {
        if (active != 'agent') {
          this.setCurrent({})
        }
      }
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
.agent-token {
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
  display: inline-block;
  padding-left: 16px;
}
.copy-icon {
  visibility: hidden;
  display: flex;
  align-items: center;
  white-space: nowrap;
  cursor: pointer;
  color: #4259ce;
}
.agentcopy {
  margin: 16px 0;
  display: flex;
  &:hover {
    .copy-icon {
      visibility: visible;
    }
  }
}
::v-deep {
  .el-dialog__wrapper.copy-agent {
    .el-dialog__header {
      display: flex;
      padding-top: 50px;
      justify-content: center;
    }
  }
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
.data-agent {
  position: relative;
}
.flexEnd {
  position: absolute;
  top: 6px;
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
  ::v-deep.el-table td.el-table__cell div {
    word-wrap: break-word;
    word-break: break-word;
  }
}
</style>
<style lang="scss">
.el-message-box__btns {
  .el-button {
    width: 80px;
  }
}
</style>
