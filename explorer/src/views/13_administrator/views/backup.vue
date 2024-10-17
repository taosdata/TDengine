<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        type="primary"
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng || $COMMUNITY"
        style="font-size:14px;"
        >{{ $t("refresh") }}</el-button
      >
      <el-tooltip
        placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
      >
        <template slot="content">
          <span v-html="$t('communityTip')"></span>
        </template>
        <el-button plain type="primary" @click="add" size="small" icon="el-icon-plus" style="font-size:14px;" :disabled="$COMMUNITY"
          >{{$t('taosuser.createbackup')}}</el-button
        >
      </el-tooltip>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column :label="$t('taosuser.directory')" width="150" prop="id" show-overflow-tooltip>
        <span slot-scope="scope">{{ scope.row.to_expand.path }}</span>
      </el-table-column>
      <el-table-column :label="$t('taosuser.database')" prop="database" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('taosuser.createtime')" prop="created_at">
        <span slot-scope="scope">{{ parsinginZone(scope.row.created_at) }}</span>
      </el-table-column>
      <el-table-column :label="$t('taosuser.lastbackup')" prop="status" show-overflow-tooltip>
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
              <span>{{
               handleDSStatus(scope.row.status)
              }}</span>
            </el-tooltip>
            <span v-else>{{
              handleDSStatus(scope.row.status)
            }}</span>
            <!-- <template v-if="scope.row.status.toLowerCase() !== 'running'">
              <el-tooltip
                placement="bottom"
                effect="light"
                content="Excute Start"
              >
                <el-button
                  plain
                  size="small"
                  @click="start(scope.row, scope.$index)"
                  icon="el-icon-qidong"
                ></el-button>
              </el-tooltip>
            </template>
            <template v-else>
              <el-tooltip
                placement="bottom"
                effect="light"
                content="Excute Stop"
              >
                <el-button
                  plain
                  size="small"
                  @click="stop(scope.row, scope.$index)"
                  icon="el-icon-tingzhi"
                ></el-button
              ></el-tooltip>
            </template> -->
          </div>
        </template>
      </el-table-column>

      <el-table-column :label="$t('taosuser.operation')" width="200">
        <template slot-scope="scope">
          <el-switch
            :value="scope.row.status.toLowerCase() != 'stopped'"
            active-color="#13ce66"
            inactive-color="#dcdfe6"
            @change="switchOperation($event, scope.row)"
            :disabled="$COMMUNITY"
          >
          </el-switch>
          <el-button
            plain
            size="small"
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
          <el-tooltip placement="top" :content="$t('taosuser.dataRestoration')" effect="light">  
            <el-button
             :disabled="scope.row.status.toLowerCase() == 'running' || $COMMUNITY"
             plain
             size="small"
             @click="handleRestorBackup(scope.row, scope.$index)"
             icon="el-icon-refresh-right"
           ></el-button>
          </el-tooltip>
          <el-button
            plain
            size="small"
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
      @close='closeDialog'
      :destroy-on-close='true'
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
        <el-form-item prop="cycle" required :label="$t('taosuser.backupcycle')">
          <el-select v-model="ruleForm.cycle" placeholder="">
            <el-option
              v-for="c in cycleList"
              :key="c.value"
              :label="$t(c.label)"
              :value="c.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('taosuser.database')" prop="db" required v-if="!isEditDialog">
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
import {
  getBackupList,
  addBackupData,
  editBackup,
  restorBackupData
} from "@/api/explorer/backup";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { validDir } from '@/utils/validate';
import { parsinginZone, decrypt } from '@/utils';
import { backupMockData } from '@/const'
export default {
  data() {
    return {
      requestIng: false,
      dblist: [],
      isEditDialog: false,
      dialogTitle: "Create New Backup",
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
          label: "taosuser.everyday",
          value: "schedule:@daily",
        },
        {
          label: "taosuser.every7day",
          value: "schedule:@weekly",
        },
        {
          label: "taosuser.every30day",
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
            required: true,
            message: this.$t('taosuser.directoryRequired'),
          },
          {
            validator: this.checkDirectory,
            trigger: "blur",  
          }
        ],
      },
      topicList: [],
      parsinginZone
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
    username() {
      return localStorage.getItem("username") || ''
    },
    decryptPwd() {
      return decrypt(localStorage.getItem("pwd")) || '';
    }
  },
  methods: {
    handlePageChange() {},
    closeDialog(){
       this.$refs.ruleForm.resetFields();
       this.$refs.ruleForm.clearValidate()
        this.dialog=false
    },
    del(data) {
      this.$confirm(
        this.$t("replication.backupDel").replace("{id}", data.id),
        this.$t("warning"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      ).then(async () => {
        await excuteDel(data.id).then((res) => {
          if (res && Object.hasOwnProperty.call(res, "id")) {
            Message({
              type: "success",
              message: this.$t('delSucc'),
            });
            this.getBackData();
          } else {
            Message({
              type: 'error',
              message: res.message
            })
          }
        });
      });
    },
    add() {
      this.dialogTitle = this.$t('taosuser.createbackup');
      this.isEditDialog = false;
      this.dialog = true;
      this.ruleForm.db = "";
      this.ruleForm.directory = "";
    },
    refresh() {
      this.getBackData();
    },
    edit(data) {
      this.dialogTitle = this.$t('taosuser.changebackup');
      this.isEditDialog = true;
      this.dialog = true;
      this.ruleForm.db = data.database;
      this.ruleForm.directory = data.to;
      this.ruleForm.cycle = `schedule:${data.trigger.schedule}`;
      this.currentRow = data;
    },
    async start(val, data) {
      try {
        await excuteStart(data.id).then((res) => {
          if (res && Object.hasOwnProperty.call(res, "code")) {
            Message({
              type: 'error',
              message: res.message
            })
          } else {
            Message.success(this.$t('operateSucc'));
            this.getBackData();
          }
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    async stop(val, data) {
      try {
        await excuteStop(data.id).then((res) => {
          if (res && Object.hasOwnProperty.call(res, "code")) {
            Message({
              type: 'error',
              message: res.message
            })
          } else {
            Message.success(this.$t('operateSucc'));
            this.getBackData();
          }
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    //切换状态
    switchOperation(val, data) {
      if (val) {
        this.$confirm(
          this.$t("replication.backupTip")
            .replace("{operate}", this.$t("replication.start"))
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
            .replace("{operate}", this.$t("replication.stop"))
            .replace("{id}", data.id),
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(()=>{
          this.stop(val, data);
        })
        
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
    async editBakcup(id) {
      //哪一项修改传参只传哪一项
      try {
        const scheduleStr = this.ruleForm.cycle;
        const [key, value] = scheduleStr.split(':');
        const scheduleObj = { [key]: value };
        
        let params = {
          trigger: scheduleObj,
        };
        await editBackup(id, params).then((res) => {
          this.getBackData();
        });
        this.dialog = false;
      } catch (err) {
        return Promise.reject(err);
      }
    },
    async addBackup() {
      try {
        const scheduleStr = this.ruleForm.cycle;
        const [key, value] = scheduleStr.split(':');
        const scheduleObj = { [key]: value };
        let base_url = localStorage.getItem("base_url")
        let splitArr = base_url.split('//')
        let dsn = splitArr[0] + "//" + this.username + ':' + encodeURIComponent(this.decryptPwd) + '@'+ splitArr[1]
        let params = {
          // name: "bakcup",
          labels: [
            "type::backup",
            `cluster-id::${localStorage.getItem("local_clusterID")}`,
          ],
          trigger: scheduleObj,
          to: `local:${this.ruleForm.directory}`,
          from: `tmq+${dsn}/${this.ruleForm.db}`,
        };
        await addBackupData(this.clusterid, params).then((res) => {
          if (res && Object.hasOwnProperty.call(res, "id")) {
            Message.success(this.$t('createSucc'));
            this.getBackData();
            this.dialog = false;
          } else {
            this.$error(res?.message)
          }
        });
      } catch (err) {
        this.$error(err);
        return Promise.reject(err);
      }
    },
    async getBackData() {
      try {
        this.requestIng = true;
        let id = localStorage.getItem("local_clusterID");
        await getBackupList(id).then((result) => {
          this.topicList = result.map((item) => {
            item["database"] = item.from.split("/").at(-1);

            return item;
          });
        });
        this.$parent.$parent.$parent.taosxDisabled = false;
        this.requestIng = false;
      } catch (error) {
        if (error.response.status == 404) {
          this.$parent.$parent.$parent.taosxDisabled = true;
        }
        if (error.response.status === 500) {
          this.$parent.$parent.$parent.taosxDisabled = true;
        }
      }
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (err) {
        return Promise.reject(err);
      }
    },
    checkDirectory(_, value, callback) {
      console.log('hshsh',value);
      if (!validDir(value)) {
        return callback(new Error(this.$t('formatWrong')));
      } else {
        callback()
      }
    },
    async restorBackup(row) {
      try {
        let params = {
          labels: [
            "type::restore",
            `cluster-id::${localStorage.getItem("local_clusterID")}`,
          ],
          force: true,
          to: row.from,
          from: row.to,
        };
        await restorBackupData(this.clusterid, params).then((res) => {
          if (res && Object.hasOwnProperty.call(res, "id")) {
            Message.success(this.$t('operateSucc'));
            this.getBackData();
          } else {
            this.$error(res?.message)
          }
        });
      } catch (err) {
        this.$error(err);
        return Promise.reject(err);
      }
    },
    handleRestorBackup(row) {
      this.$confirm(
        this.$t('taosuser.isRestore'),
        this.$t("warning"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      ).then(()=>{
        this.restorBackup(row);
      })
    },
    handleDSStatus(value) {
      return this.$t('statuses.' + value);
    },
  },
  created() {
    if (this.$COMMUNITY) {
      this.topicList = backupMockData
    } else {
      this.getDatabases();
      this.getBackData();
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
