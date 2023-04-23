<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng"
        >{{ $t("refresh") }}</el-button
      >
      <el-button plain @click="add" size="small" icon="el-icon-plus">{{
        $t("taosagents.createnewagent")
      }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column label="ID" width="150" prop="id"></el-table-column>
      <el-table-column
        :label="$t('taosagents.cluster_id')"
        prop="cluster_id"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.connectors')"
        prop="connectors"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.created_at')"
        prop="created_at"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.dsn')"
        prop="dsn"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.expire_date')"
        prop="expire_date"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.connectors')"
        prop="connectors"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.last_modified_at')"
        prop="last_modified_at"
      ></el-table-column>
      <el-table-column
        :label="$t('taosagents.name')"
        prop="name"
      ></el-table-column>
      <el-table-column :label="$t('taosagents.status')" prop="status">
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
      </el-table-column>
      <el-table-column
        :label="$t('taosagents.user_id')"
        prop="user_id"
      ></el-table-column>
      <el-table-column :label="$t('taosuser.operation')" width="150">
        <template slot-scope="scope">
          <el-switch
            :value="scope.row.status.toLowerCase() == 'running'"
            active-color="rgb(66, 89, 206)"
            inactive-color="#dcdfe6"
            @change="switchOperation($event, scope.row)"
          >
          </el-switch>
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
        <el-form-item prop="cycle" required :label="$t('taosuser.backupcycle')">
          <el-select v-model="ruleForm.cycle" placeholder="">
            <el-option
              v-for="c in cycleList"
              :key="c.value"
              :label="c.label"
              :value="c.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item
          :label="$t('taosuser.database')"
          prop="db"
          required
          v-if="!isEditDialog"
        >
          <el-select v-model="ruleForm.db" placeholder="">
            <el-option
              v-for="db in dblist"
              :key="db['node-key']"
              :label="db.name"
              :value="db.name"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item
          :label="$t('taosuser.directory')"
          prop="directory"
          required
          v-if="!isEditDialog"
        >
          <el-input v-model.trim="ruleForm.directory"></el-input>
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
  </div>
</template>
<script>
import { getAgentsData } from "@/api/explorer/agent";
export default {
  name: "Agent",
  data() {
    return {
      requestIng: false,
      dblist: [],
      isEditDialog: false,
      dialogTitle: "Create New Agent",
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      operateStatus: true,
      currentRow: null,
      clusterid: localStorage.getItem("local_clusterID"),
      ruleForm: {
        cycle: "",
        db: "",
        directory: "",
      },
      cycleList: [
        {
          label: "Everyday",
          value: "schedule:@daily",
        },
        {
          label: "Every 7 days",
          value: "schedule:@weekly",
        },
        {
          label: "Every 30 days",
          value: "schedule:@monthly",
        },
      ],
      rules: {
        cylce: [
          {
            message: "Please select backup cycle",
            trigger: "change",
          },
        ],
        db: [
          {
            message: "Please select the database",
            trigger: "change",
          },
        ],
        directory: [
          {
            message: "Please enter the directory",
            trigger: "blur",
          },
        ],
      },
      topicList: [],
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.cycle) {
        return true;
      }

      if (!this.ruleForm.db) {
        return true;
      }
      if (!this.ruleForm.directory) {
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
        "Are you sure  to delete " + data.id + " backup task?",
        "Warning",
        {
          confirmButtonText: "Ok",
          cancelButtonText: "Cancel",
          type: "warning",
        }
      ).then(async () => {});
    },
    add() {
      this.dialogTitle = this.$t("taosagents.createnewagent");
      this.isEditDialog = false;
      this.dialog = true;
      this.ruleForm.db = "";
      this.ruleForm.directory = "";
    },
    refresh() {
      this.getBackData();
    },
    edit(data) {
      this.dialogTitle = this.$t("taosagents.changebackup");
      this.isEditDialog = true;
      this.dialog = true;
      this.ruleForm.db = data.database;
      this.ruleForm.directory = data.to;
      this.currentRow = data;
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
        this.editBakcup(this.currentRow.id);
      } else {
        this.addBackup();
      }
    },

    async getAgents() {
      try {
        let result = await getAgentsData(
          localStorage.getItem("local_clusterID"),
          localStorage.getItem("username")
        );
        console.log(result, "获取agents----");
      } catch (error) {
        console.log(error);
      }
    },
  },
  created() {
    this.getAgents();
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
