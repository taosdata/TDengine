<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="refreshable"
      >
        {{ $t("refresh") }}
      </el-button>
      <el-button plain @click="add" size="small" icon="el-icon-plus"
        >{{$t('taosuser.addreplication')}}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column label="ID" width="80" prop="id"></el-table-column>
      <el-table-column :label="$t('taosuser.fromdb')" prop="fromdb"></el-table-column>
      <el-table-column :label="$t('taosuser.toinstance')" prop="hostport"></el-table-column>
      <el-table-column :label="$t('taosuser.todb')" prop="db"></el-table-column>

      <el-table-column :label="$t('taosuser.status')" prop="status"></el-table-column>
      <el-table-column :label="$t('taosuser.reason')" prop="reason"></el-table-column>
      <el-table-column :label="$t('taosuser.finishat')" prop="finished_at"></el-table-column>
      <el-table-column :label="$t('taosuser.createat')" prop="created_at"></el-table-column>
      <el-table-column :label="$t('taosuser.operation')" width="110">
        <template slot-scope="scope">
          <el-switch
            :value="scope.row.status.toLowerCase() == 'running'"
            active-color="rgb(66, 89, 206)"
            inactive-color="#dcdfe6"
            @change="switchOperation($event, scope.row)"
          ></el-switch>
          <!-- <el-button
            plain
            size="small"
            @click="edit(scope.row)"
            icon="el-icon-edit"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="start(scope.row, scope.$index)"
            icon="el-icon-qidong"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="stop(scope.row, scope.$index)"
            icon="el-icon-tingzhi"
          ></el-button>-->
          <el-button
            plain
            size="small"
            @click="del(scope.row, scope.$index)"
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
      :title="$t('taosuser.addreplication')"
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
        label-width="auto"
        class="demo-ruleForm"
      >
        <el-form-item :label="$t('taosuser.fromsource')" prop="source" required>
          <!-- <el-input v-model.trim="ruleForm.source"></el-input> -->
          <el-select v-model="ruleForm.source" :placeholder="$t('pleaseSelect')">
            <el-option
              v-for="db in dblist"
              :key="db['node-key']"
              :label="db.name"
              :value="db.name"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('taosuser.targetdsn')" prop="target" required>
          <el-input
            v-model.trim="ruleForm.target"
            placeholder="taos://192.168.0.1:6030/db2"
          ></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">
            {{ $t("cancel") }}
          </el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            @click="addReplication"
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
import { Message } from "element-ui";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import {
  getReplicationList,
  addReplicationData,
} from "@/api/explorer/replication";
import _ from "lodash";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import taosbenchmarkVue from "@/utils/config/mdx/taosbenchmark.vue";
export default {
  data() {
    return {
      refreshable: false,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      dblist: [],
      ruleForm: {
        source: "",
        target: "",
      },
      rules: {
        source: [
          {
            required: true,
            message: this.$t('taosuser.fromsourceRequired'),
          },
        ],
        target: [
          {
            required: true,
            message: this.$t('taosuser.targetdsnRequired'),
          },
        ],
      },
      topicList: [],
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.source) {
        return true;
      }
      if (!this.ruleForm.target) {
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
    add() {
      this.dialog = true;
      this.ruleForm.source = "";
      this.ruleForm.target = "";
    },
    del(data) {
      this.$confirm(
        this.$t("replication.backupDel").replace("{id}",data.id), 
        this.$t("warning"),
      {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      }).then(async () => {
        await excuteDel(data.id).then(() => {
          Message({
            type: "success",
            message: this.$t('delSucc'),
          });
          this.getReplication();
        });
      });
    },
    refresh() {
      this.refreshable = true;
      this.getReplication();
    },
    async addReplication() {
      try {
        let id = localStorage.getItem("local_clusterID");
        let params = {
          name: "replication",
          labels: [
            "type::replication",
            `cluster-id::${localStorage.getItem("local_clusterID")}`,
          ],
          to: `${this.ruleForm.target}`,
          from: `tmq+${localStorage.getItem("base_url")}/${
            this.ruleForm.source
          }`,
        };
        await addReplicationData(id, params).then((res) => {
          if (res) {
            Message.success(this.$t('createSucc'));
            this.getReplication();
          }
          this.dialog = false;
        });
      } catch (err) {
        Message.error(err?.message);
        return Promise.reject(err);
      }
    },
    edit(data) {
      this.dialog = taosbenchmarkVue;
      this.ruleForm.source = data.source;
      this.ruleForm.target = data.target;
    },
    async start(val, data) {
      try {
        await excuteStart(data.id).then((res) => {
          Message.success(this.$t('operateSucc'));
          this.getReplication();
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    async stop(val, data) {
      try {
        await excuteStop(data.id).then(() => {
          Message.success(this.$t('operateSucc'));
          this.getReplication();
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    switchOperation(val, data) {
      this.$confirm(
        `${this.$t(val ? this.$t('replication.start') : this.$t('replication.stop'))} ${this.$t(
          "replication.theTaskWithId"
        ).replace("{id}", data.id)}?`,
        this.$t("warning"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      ).then(() => {
        if (val) {
          this.start(val, data);
        } else {
          this.stop(val, data);
        }
      });
    },
    async getReplication() {
      try {
        let id = localStorage.getItem("local_clusterID");
        await getReplicationList(id).then((result) => {
          this.topicList = result.map((item) => {
            let to_port = _.get(item, "to_expand.port");
            item["fromdb"] = item.from.split("/").at(-1);
            item["hostport"] =
              _.get(item, "to_expand.host") ||
              "localhost" + (to_port ? `:${to_port}` : "");
            item["db"] = item.to_expand
              ? item.to_expand.subject
              : item["fromdb"];
            return item;
          });
        });
        this.$parent.$parent.$parent.taosxDisabled = false;
      } catch (error) {
        if (error.response.status == 404) {
          this.$parent.$parent.$parent.taosxDisabled=true
        }
        if (error.response.status === 500) {
          this.$parent.$parent.$parent.taosxDisabled=true
        }
      }
      this.refreshable = false;

    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (error) {
        console.log(error);
      }
    },
  },
  created() {
    this.getDatabases();
    this.getReplication();
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
