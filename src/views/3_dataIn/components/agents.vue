<template>
  <div class="dnode-block">
    <div style="font-size: 18px" v-if="agentList?.length > 0">
      <p class="title">
        <span>{{ $t("topic.agent") }}</span>
      </p>
    </div>
    <el-table
      v-if="agentList?.length > 0"
      style="margin-top: 20px"
      :data="agentList"
      size="mini"
      max-height="250"
    >
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

      <el-table-column
        :label="$t('taosagents.created_at')"
        prop="created_at"
      ></el-table-column>
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
            size="small"
            @click="edit(scope.row, scope.$index)"
            icon="el-icon-edit"
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
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
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
      width="600px"
      :visible.sync="copyDialog"
      :destroy-on-close="true"
      :close-on-click-modal="false"
    >
      <div style="display: flex" class="agentcopy">
        <span class="agent-token">{{ agenttoken }}</span>
        <span class="copy-icon" @click="copyToken(agenttoken)">
          <i class="el-icon-copy-document"></i>
          {{ $t("copy") }}
        </span>
      </div>
    </el-dialog>
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
import { getUIData } from "@/api/explorer/datain";
import { format } from "date-fns";
import { Message } from "element-ui";
export default {
  name: "Agent",
  data() {
    return {
      expireTimeOPtion: {
        disabledDate(time) {
          return time.getTime() < Date.now();
        },
      },

      agenttoken: "",

      requestIng: false,
      dblist: [],
      isEditDialog: false,
      dialogTitle: "Create New Agent",
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      copyDialog: false,
      operateStatus: true,
      currentRow: null,
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
    handlePageChange() {},
    closeDialog() {
      this.$refs.ruleForm.resetFields();
      this.$refs.ruleForm.clearValidate();
      this.dialog = false;
    },
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
              res && res.message && Message.error(res.message);
            })
            .catch((err) => {
              err.response.data &&
                err.response.data.message &&
                Message.error(err.response.data.message);
            });
        } catch (err) {
          err.response.data &&
            err.response.data.message &&
            Message.error(err.response.data.message);
        }

        this.getAgents();
      });
    },
    add() {
      this.dialogTitle = this.$t("taosagents.createnewagent");
      this.isEditDialog = false;
      this.dialog = true;
      this.ruleForm.name = "";
      this.ruleForm.expire_date = "";
      this.ruleForm.connectors = "";
    },
    refresh() {
      this.getAgents();
    },
    edit(data) {
      this.dialogTitle = this.$t("taosagents.edittitle");
      this.isEditDialog = true;
      this.dialog = true;
      this.ruleForm.name = data.name;
      this.ruleForm.connectors = data.connectors;
      this.ruleForm.expire_date = data.expire_date;
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
        this.getAgents();
        if (result.token) {
          this.agenttoken = result.token;
          this.copyDialog = true;
        }
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
        this.agentList = (
          await getAgentsData(
            localStorage.getItem("local_clusterID"),
            localStorage.getItem("username")
          )
        ).map((item) => {
          item["created_at"] = item.created_at
            ? item.created_at.replace(/(?<=\.)\S+$/, "").replace(".", "") + "Z"
            : "";
          return item;
        });
      } catch (error) {
        console.log(error);
      }
    },
    async getConnectorTypes() {
      try {
        let result = await getUIData();
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
        this.getAgents();
        if (result.token) {
          this.agenttoken = result.token;
          this.copyDialog = true;
        }
      } catch (error) {
        console.log(error);
      }
    },
  },
  created() {
    this.getAgents();
    this.getConnectorTypes();
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
  padding: 8px 16px;
}
</style>
