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
      <el-table-column width="100" :label="$t('taosuser.database')" prop="database" show-overflow-tooltip></el-table-column>
      <el-table-column width="120" :label="$t('topic.stables')" prop="stable" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('taosuser.backupForm.fileDir')" prop="directory" show-overflow-tooltip></el-table-column>
      <el-table-column width="210" :label="$t('taosuser.backupForm.upcoming')" prop="upcoming">
        <span slot-scope="scope">{{ formatTime(scope.row.upcoming) }}</span>
      </el-table-column>
      <el-table-column width="60" :label="$t('taosuser.lastbackup')" prop="status" show-overflow-tooltip>
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
            @click="edit(scope.row)"
            icon="el-icon-edit"
            :disabled="$COMMUNITY"
          ></el-button>
          <el-tooltip placement="top" :content="$t('taosuser.dataRestoration')" effect="light">  
            <el-button
             :disabled="scope.row.status.toLowerCase() == 'running' || $COMMUNITY"
             plain
             size="small"
             @click="handleRestorBackup(scope.row, scope.$index)"
             icon="el-icon-first-aid-kit"
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
        <el-form-item :label="$t('taosuser.database')" prop="database">
          <el-select v-model="ruleForm.database" @change="getSTbaleList"  :disabled="!!currentId">
            <el-option
              v-for="db in dblist"
              :key="db['node-key']"
              :label="db.name"
              :value="db.name"
            >
            </el-option>
          </el-select>
        </el-form-item>

        <el-form-item :label="$t('datasource.supertable')" prop="stable">
          <el-select
            v-model="ruleForm.stable"
            allow-create
            default-first-option
            size="small"
            :disabled="!!currentId"
          >
            <el-option
              v-for="(item, index) in stableList"
              :key="`stable-option-${index}`"
              :label="item"
              :value="item"
            ></el-option>
          </el-select>
        </el-form-item>

        <el-form-item :label="$t('taosuser.backupForm.upcoming')" required prop="upcoming" style="text-align: left;">
          <el-date-picker
            v-model="ruleForm.upcoming"
            type="datetime"
            value-format="yyyy-MM-ddTHH:mm:ss">
          </el-date-picker>
        </el-form-item>

        <el-form-item prop="interval_value" required :label="$t('taosuser.backupcycle')">
          <el-input v-model="ruleForm.interval_value" class="input-with-select">
            <el-select v-model="ruleForm.interval_unit" style="width: 100px;" slot="append">
              <el-option :label="$t('dashboard.timeUnit')[2]" value="h"></el-option>
              <el-option :label="$t('dashboard.timeUnit')[3]" value="d"></el-option>
            </el-select>
          </el-input>
        </el-form-item>
        <el-form-item prop="max_retry" required :label="$t('taosuser.backupForm.maxRetry')">
          <el-input v-model="ruleForm.max_retry"></el-input>
        </el-form-item>
        <el-form-item prop="retry_interval" required :label="$t('taosuser.backupForm.retryInterval')">
          <el-input v-model="ruleForm.retry_interval">
            <template slot="append">{{ $t('dashboard.timeUnit')[0] }}</template>
          </el-input>
        </el-form-item>

        <el-form-item
          :label="$t('taosuser.directory')"
          prop="directory"
        >
          <el-input v-model.trim="ruleForm.directory" :disabled="!!currentId"></el-input>
        </el-form-item>
        <el-form-item prop="backup_max_size_value" required :label="$t('taosuser.backupForm.backupMaxSize')">
          <el-input v-model="ruleForm.backup_max_size_value">
            <el-select v-model="ruleForm.backup_max_size_unit" style="width: 100px;" slot="append">
              <el-option label="MB" value="MB"></el-option>
              <el-option label="GB" value="GB"></el-option>
            </el-select>
          </el-input>
        </el-form-item>
        <el-form-item prop="compression_level" :label="$t('taosuser.backupForm.compressionLevel')">
          <el-select v-model="ruleForm.compression_level">
            <el-option :label="$t('taosuser.compressionLevel.balanced')" value="balanced"></el-option>
            <el-option :label="$t('taosuser.compressionLevel.best')" value="best"></el-option>
            <el-option :label="$t('taosuser.compressionLevel.fastest')" value="fastest"></el-option>
          </el-select>
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
            @click="saveBakcup"
            v-loading="requestIng"
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
import { getDBListReq, getStables } from "@/api/gateway/data/dbs.js";
import { validDir } from '@/utils/validate';
import { parsinginZone, decrypt, getTimezoneAddition } from '@/utils';
import { backupMockData } from '@/const';
export default {
  data() {
    return {
      requestIng: false,
      dblist: [],
      stableList: [],
      dialogTitle: "Create New Backup",
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      operateStatus: true,
      currentId: null,
      clusterid: localStorage.getItem("local_clusterID"),
      ruleForm: {
        database: "",
        stable: "",
        upcoming: "",
        interval_value: "1",
        interval_unit: "d",
        directory: "",
        max_retry: 3,
        retry_interval: 5,
        backup_max_size_value: "1",
        backup_max_size_unit: "GB",
        compression_level: "balanced",
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
        database: [
          {
            required: true,
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
    username() {
      return localStorage.getItem("username") || ''
    },
    decryptPwd() {
      return decrypt(localStorage.getItem("pwd")) || '';
    }
  },
  methods: {
    async getSTbaleList() {
      this.stableList = await getStables(this.ruleForm.database);
    },
    closeDialog(){
      this.$refs.ruleForm.resetFields();
      this.$refs.ruleForm.clearValidate()
      this.dialog=false
    },
    formatTime(t) {
      let timeFMT = parsinginZone(t);
      let plus_index = timeFMT.indexOf("+");
      if (plus_index > 0) {
        timeFMT = timeFMT.substring(0, plus_index);
      }
      return timeFMT;
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
      this.dialog = true;
      this.ruleForm = {
        database: "",
        stable: "",
        upcoming: "",
        interval: "1d",
        interval_value: "1",
        interval_unit: "d",
        directory: "",
        max_retry: 3,
        retry_interval: 5,
        backup_max_size: "1GB",
        backup_max_size_value: "1",
        backup_max_size_unit: "GB",
        compression_level: "balanced",
      }
      this.currentId = null;
    },
    refresh() {
      this.getBackData();
    },
    edit(data) {
      this.dialogTitle = this.$t('taosuser.changebackup');
      this.dialog = true;
      this.currentId = data.id;
      this.ruleForm.database = data.database;
      this.ruleForm.stable = data.stable;
      this.ruleForm.upcoming = data.upcoming;
      this.ruleForm.directory = data.directory;
      this.ruleForm.compression_level = data.compression_level;
      this.ruleForm.max_retry = data.max_retry;
      this.ruleForm.retry_interval = data.retry_interval;

      const interval_parts = data.interval.match(/^(\d+)([sdh])$/);
      if (interval_parts && interval_parts.length === 3) {
        this.ruleForm.interval_value = interval_parts[1];
        this.ruleForm.interval_unit = interval_parts[2];
      }
      const backup_file_max_size_parts = data.max_size.match(/^(\d+)([A-Z]{2})/);
      this.ruleForm.backup_max_size_value = backup_file_max_size_parts[1];
      this.ruleForm.backup_max_size_unit = backup_file_max_size_parts[2];
    },
    parseData(data, targetData) {
      targetData.id = data.id;
      targetData["database"] = data.from.split("/").at(-1);
      let params_start = targetData["database"].indexOf("?");
      if (params_start > 0) {
        targetData["database"] = targetData["database"].substring(0, params_start);
      }

      targetData.status = data.status;
      targetData.stable = data.from_expand.params.stable;
      targetData.upcoming = new Date(data.trigger.upcoming);

      targetData.interval = data.trigger.interval;
      targetData.max_size = data.to_expand.params.max_size;

      targetData.directory = data.to_expand.path;
      targetData.max_retry = data.from_expand.params.max_retry;
      const retry_interval_part = data.from_expand.params.retry_interval.match(/^(\d+)s$/);
      if (retry_interval_part && retry_interval_part.length === 2) {
        targetData.retry_interval = retry_interval_part[1];
      }
      targetData.backup_max_size = data.to_expand.params.max_size;
      targetData.compression_level = data.to_expand.params.compression_level;
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
    saveBakcup() {
      this.$refs.ruleForm.validate(async (valid) => {
        if (valid) {
          const postData = this.constructPostData();
          if (this.currentId) {
            await editBackup(this.currentId, postData);
          } else {
            await addBackupData(postData);
          }
          
          Message.success(this.$t('editSucc'));
          this.dialog = false;
          this.refresh();
        }
      });
    },
    constructPostData() {
      const clusterID = localStorage.getItem("local_clusterID");
      
      let base_url = localStorage.getItem("base_url")
      let splitArr = base_url.split('//')
      let dsn = `tmq+${splitArr[0]}//${this.username}:${encodeURIComponent(this.decryptPwd)}@${splitArr[1]}/${this.ruleForm.database}`;
      dsn += `?max_retry=${this.ruleForm.max_retry}&retry_interval=${this.ruleForm.retry_interval}s`;
      if (this.ruleForm.stable) {
        dsn += `&stable=${this.ruleForm.stable}`;
      }

      return {
        "labels": [
          "type::backup",
          `cluster-id::${clusterID}`
        ],
        "trigger": {
          "schedule": "@daily",
          "upcoming": `${this.ruleForm.upcoming}${getTimezoneAddition()}`, 
          "interval": `${this.ruleForm.interval_value}${this.ruleForm.interval_unit}`
        },
        "from": dsn,
        "to": `local:${this.ruleForm.directory}?max_size=${this.ruleForm.backup_max_size_value}${this.ruleForm.backup_max_size_unit}&compression_level=${this.ruleForm.compression_level}`
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
          from: dsn,
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
            let targetData = {};
            this.parseData(item, targetData);
            return targetData;
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
