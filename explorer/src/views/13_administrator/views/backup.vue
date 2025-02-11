<template>
  <div class="dnode-block">
    
    <el-tabs v-model="backupActiveTab" :lazy="true">
      <el-tab-pane :label="$t('taosuser.backupPlan')" name="backupPlan">
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
        <el-table style="margin-top: 20px" :data="topicList">
          <el-table-column width="50" label="ID" prop="id" show-overflow-tooltip></el-table-column>
          <el-table-column width="150" :label="$t('taosuser.database')" prop="database" show-overflow-tooltip></el-table-column>
          <el-table-column width="180" :label="$t('topic.stables')" prop="stable" show-overflow-tooltip></el-table-column>
          <el-table-column :label="$t('taosuser.backupForm.fileDir')" prop="directory" show-overflow-tooltip></el-table-column>
          <el-table-column width="100" :label="$t('taosuser.backupFile')" prop="upcoming" align="center">
            <a slot-scope="scope" @click="activeBackupFileOf(scope.row.id)">{{ $t('view') }}</a>
          </el-table-column>
          <el-table-column width="100" :label="$t('taosuser.lastbackup')" prop="status" show-overflow-tooltip>
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
              </div>
            </template>
          </el-table-column>
          <el-table-column :label="$t('taosuser.operation')" width="280">
            <template slot-scope="scope">
              <el-switch
                :value="scope.row.status.toLowerCase() != 'stopped'"
                active-color="#13ce66"
                inactive-color="#dcdfe6"
                @change="switchOperation($event, scope.row, 'replication.backupTip')"
                :disabled="$COMMUNITY"
              >
              </el-switch>
              <el-button
                plain
                size="small"
                @click="viewBackup(scope.row)"
                icon="el-icon-view"
              ></el-button>
              <el-button
                plain
                size="small"
                @click="edit(scope.row)"
                icon="el-icon-edit"
                :disabled="$COMMUNITY"
              ></el-button>
              <el-button
                plain
                size="small"
                @click="copy(scope.row)"
                icon="el-icon-document-copy"
              ></el-button>
              <el-button
                plain
                size="small"
                @click="toDel(scope.row)"
                icon="el-icon-delete"
                :disabled="$COMMUNITY || scope.row.status.toLowerCase() != 'stopped'"
              ></el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
      <el-tab-pane :label="$t('taosuser.backupFile')" name="backupFile">
        <div class="flaxStart">
          <el-select v-model="currentId" style="width: 350px;" @change="showBackupHistory">
            <el-option 
              v-for="plan in topicList" 
              v-bind:key="`filterBackupFile-${plan.id}`"
              :label="`${plan.id} | ${plan.database} ${plan.stable ? '| ' + plan.stable : ''}`" 
              :value="plan.id"></el-option>
          </el-select>
        </div>
        <el-table style="margin-top: 20px" :data="historyList" default-expand-all>
          <el-table-column :label="$t('taosuser.backupPoint')" prop="point">
            <span slot-scope="scope">{{ parsinginZone(scope.row.point) }}</span>
          </el-table-column>
          <el-table-column width="180" :label="$t('taosuser.backupFileSize')" prop="file_size"></el-table-column>
          <el-table-column width="180" :label="$t('taosuser.backupFileCount')" prop="file_count"></el-table-column>
          <el-table-column width="100" :label="$t('taosuser.operation')">
            <el-tooltip placement="top" :content="$t('taosuser.dataRestoration')" slot-scope="scope" effect="light">  
              <el-button
              plain
              size="small"
              @click="toRestoreBackup(scope.row)"
              icon="el-icon-first-aid-kit"
            ></el-button>
            </el-tooltip>
          </el-table-column>
        </el-table>
      </el-tab-pane>
      <el-tab-pane :label="$t('taosuser.restoreTask')" name="restoreTask">
        <div class="flexEnd">
          <el-button
            plain
            type="primary"
            @click="refreshRestoreTask"
            size="small"
            icon="el-icon-refresh"
            :disabled="requestIng || $COMMUNITY"
            style="font-size:14px;"
            >{{ $t("refresh") }}</el-button
          >
        </div>
        <el-table style="margin-top: 20px" :data="restoreList">
          <el-table-column width="50" label="ID" prop="id" show-overflow-tooltip></el-table-column>
          <el-table-column width="150" :label="$t('taosuser.backupForm.fileDir')" prop="from_path" show-overflow-tooltip></el-table-column>
          <el-table-column width="420" :label="$t('taosuser.restoreRange')" prop="stable" show-overflow-tooltip>
            <template slot-scope="scope">
              <span>{{ parsinginZone(scope.row.from_point_start) }} ~ {{ parsinginZone(scope.row.from_point_end) }}</span>
            </template>
          </el-table-column>
          <el-table-column :label="$t('taosuser.todb')" prop="to_database" show-overflow-tooltip></el-table-column>
          <el-table-column width="220" :label="$t('taosuser.createtime')" prop="upcoming" align="center">
            <span slot-scope="scope">{{ parsinginZone(scope.row.created_at) }}</span>
          </el-table-column>
          <el-table-column width="100" :label="$t('taosuser.lastbackup')" prop="status" show-overflow-tooltip>
            <template slot-scope="scope">
              <span>{{ handleDSStatus(scope.row.status) }}</span>
            </template>
          </el-table-column>
          <el-table-column :label="$t('taosuser.operation')" width="150">
            <template slot-scope="scope">
              <el-switch
                :value="scope.row.status.toLowerCase() != 'stopped'"
                :disabled="$COMMUNITY || scope.row.status.toLowerCase() == 'completed'"
                active-color="#13ce66"
                inactive-color="#dcdfe6"
                @change="switchOperation($event, scope.row, 'replication.restoreTip')"
              >
              </el-switch>
              <el-button
                plain
                size="small"
                @click="toDeleteRestoreTask(scope.row.id)"
                icon="el-icon-delete"
                :disabled="$COMMUNITY || scope.row.status.toLowerCase() != 'stopped'"
              ></el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>
    
    <el-dialog
      align="center"
      :title="dialogTitle"
      width="600px"
      :visible.sync="dialog"
      @close='closeDialog'
      :destroy-on-close='true'
      :close-on-click-modal="false"
    >
      <div class="cover-readonly" v-if="viewOnly"></div>
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        :label-width="$i18n.locale=='zh'? '120px': '180px'"
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
            :value-format="`yyyy-MM-ddTHH:mm:ss${getTimezoneAddition()}`">
          </el-date-picker>
        </el-form-item>

        <el-form-item prop="interval_value" required :label="$t('taosuser.backupcycle')">
          <el-input v-model="ruleForm.interval_value" class="input-with-select">
            <el-select v-model="ruleForm.interval_unit" style="width: 100px;" slot="append">
              <el-option :label="$t('dashboard.timeUnit')[1]" value="m"></el-option>
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
        <el-form-item v-if="viewOnly" prop="created_at" :label="$t('taosuser.createtime')">
          <el-input v-model="ruleForm.created_at"></el-input>
        </el-form-item>
      </el-form>
      
      <el-row style="margin-top: 20px" v-if="!viewOnly">
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
    
    <el-dialog
      :title="$t('tips')"
      :visible.sync="deleteConfirmDialog"
      width="400px">
      <span><el-checkbox v-model="yesDeleteFile">{{ $t('taosuser.confirmDeleteBackupFile') }}</el-checkbox></span>
      <span slot="footer" class="dialog-footer">
          <el-button size="small" @click="deleteConfirmDialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
 
          <el-button
            size="small"
            @click="del()"
            v-loading="requestIng"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
      </span>
    </el-dialog>
    <el-dialog
      :title="$t('tips')"
      :visible.sync="restoreConfirmDialog"
      width="650px">
      <div> 
        <div style="margin-bottom: 10px;">
          {{ $t('taosuser.confirmRestoreRange') }}
          <el-select v-model="restoreRange.from" style="width: 230px;">
            <el-option
              v-for="item in restoreRangeList"
              :key="item"
              :label="parsinginZone(item)"
              :value="item"
            ></el-option>
          </el-select>
          <span> ~ </span>
          {{ parsinginZone(restoreRange.to) }}
        </div>
        <div>
          {{ $t('taosuser.restoreToDatabase') }}
          <el-select v-model="ruleForm.database" style="width: 230px;">
            <el-option
              v-for="db in dblist"
              :key="db['node-key']"
              :label="db.name"
              :value="db.name"
            >
            </el-option>
          </el-select>
        </div>
      </div>
      
      <div slot="footer" class="dialog-footer">
          <el-button size="small" @click="restoreConfirmDialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
 
          <el-button
            size="small"
            @click="restoreBackup()"
            v-loading="requestIng"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
        </div>
    </el-dialog>
  </div>
</template>
<script>
import {
  getBackupList,
  addBackupData,
  editBackup,
  restorBackupData,
  getBackupHistory,
  restoreBackups,
} from "@/api/explorer/backup";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import { Message } from "element-ui";
import { getDBListReq, getStables } from "@/api/gateway/data/dbs.js";
import { validDir } from '@/utils/validate';
import { getMetrics } from "@/api/explorer/datain";
import { parsinginZone, decrypt, getTimezoneAddition } from '@/utils';
import { backupMockData } from '@/const';
export default {
  data() {
    return {
      requestIng: false,
      dblist: [],
      stableList: [],
      historyList: [],
      restoreRangeList: [],
      backupActiveTab: "backupPlan",
      restoreRange: {
        from: "",
        to: "",
      },
      dialogHistory: false,
      deleteConfirmDialog: false,
      restoreConfirmDialog: false,
      yesDeleteFile: false,
      dialogTitle: "",
      viewOnly: false,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      operateStatus: true,
      currentId: null,
      clearBackupFile: false,
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
        compression_level: "fastest",
        created_at: "",
      },
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
      restoreList: [],
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
    toRestoreBackup(toFile) {
      this.restoreRangeList = this.historyList.map(item => item.point).filter(item => item <= toFile.point);
      this.restoreRange.from = toFile.point;
      this.restoreRange.to =  toFile.point;
      this.pointToRestore = toFile;
      this.restoreConfirmDialog = true;
    },
    async restoreBackup() {
      if (!this.ruleForm.database) {
        Message.warn(this.$t('taosuser.selectDatabase'));
        return;
      }
      let backupDirectory = null;
      for (let i = 0; i < this.topicList.length; i++) {
        if (this.topicList[i].id === this.currentId) {
          backupDirectory = this.topicList[i].directory;
          break;
        }
      }

      try {
        let res = await restoreBackups({
          from: this.restoreRange.from,
          to: this.restoreRange.to,
          database: this.ruleForm.database,
          point: this.pointToRestore,
          backupDirectory,
        });

        if (res && res.code) {
          Message.error(res.message);
          return;
        }

        Message.success(this.$t('operateSucc'));
        this.restoreConfirmDialog = false;
        await this.getRestoreTasks();
        this.backupActiveTab = "restoreTask";
      } catch (err) {
        Message.error(err);
      }
    }, 
    getTimezoneAddition() {
      return getTimezoneAddition();
    },
    async activeBackupFileOf(id) {
      this.currentId = id;
      try {
        const res = await getBackupHistory(this.currentId);
        if (res && res.code > 0) {
          Message.error(res.message);
        } else {
          this.historyList = res;
          this.backupActiveTab = "backupFile";
        }
      } catch (err) {
        Message.error(err);
      }
    },
    async displayMetrics(id) {
      getMetrics(id);
    },
    async showBackupHistory() {
      try {
        const res = await getBackupHistory(this.currentId);
        if (res && res.code > 0) {
          Message.error(res.message);
        } else {
          this.historyList = res;
        }
      } catch (err) {
        Message.error(err);
      }
      
      // let currentItem = {"id": 0, "point": res[0].point, "file_size": 0, "file_count": 0, "hasChildren":true, "children": []};
      // const groupedList = [currentItem];

      // for (let i = 0; i < res.length; i++) {
      //   let item = res[i];
      //   if (item.point === currentItem.point) {
      //     currentItem.file_size = item.file_size;
      //     currentItem.file_count += item.file_count;
      //     currentItem.children.push(item);
      //     currentItem.children.push(item);
      //   } else {
      //     currentItem = {"id": i, "point": item.point, "hasChildren":true, "file_size": item.file_size, "file_count": item.file_count, "children": [item]};
      //     groupedList.push(currentItem);
      //   }
      // }
      // this.historyList = groupedList;
      // console.log('this.historyList', groupedList);
      // this.dialogHistory = true;
    },
    closeDialog(){
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
    toDel(row) {
      this.currentId = row.id;
      this.deleteConfirmDialog = true;
      this.yesDeleteFile = false;
    },
    toDeleteRestoreTask(id) {
      this.$confirm(
          this.$t('taosuser.conformDeleteRestoreTask') + id + '?',
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(() => {
          this.currentId = id;
          this.del();
        });
    },

    del() {
      excuteDel(this.currentId, this.yesDeleteFile).then((res) => {
        if (res && Object.hasOwnProperty.call(res, "id")) {
          Message({
            type: "success",
            message: this.$t('delSucc'),
          });
          this.deleteConfirmDialog = false;
          this.getBackData();
          this.getRestoreTasks();
        } else {
          Message({
            type: 'error',
            message: res.message
          })
        }
      });
    },
    add() {
      this.dialogTitle = `${this.$t('create')} ${this.$t('taosuser.backupPlan')}`;
      this.dialog = true;
      this.viewOnly = false;
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
        compression_level: "fastest",
      }
      this.currentId = null;
      this.$refs.ruleForm.clearValidate();
    },
    refresh() {
      this.getBackData();
    },
    refreshRestoreTask() {
      this.getRestoreTasks();
    },
    edit(data) {
      this.copy(data);
      this.ruleForm.database = data.database;
      this.ruleForm.stable = data.stable;
      this.dialogTitle = `${this.$t('change')} ${this.$t('taosuser.backupPlan')}`;
      this.currentId = data.id;
    },
    copy(data) {
      this.currentId = null;
      this.viewOnly = false;
      this.dialogTitle = `${this.$t('create')} ${this.$t('taosuser.backupPlan')}`;
      this.dialog = true;
      this.ruleForm.database = "";
      this.ruleForm.stable = "";
      this.ruleForm.upcoming = data.upcoming;
      this.ruleForm.directory = data.directory;
      this.ruleForm.compression_level = data.compression_level;
      this.ruleForm.max_retry = data.max_retry;
      this.ruleForm.retry_interval = data.retry_interval;
      
      const interval_parts = data.interval.match(/^(\d+)([smhd])$/);
      if (interval_parts && interval_parts.length === 3) {
        this.ruleForm.interval_value = interval_parts[1];
        this.ruleForm.interval_unit = interval_parts[2];
      }
      const backup_file_max_size_parts = data.max_size.match(/^(\d+)([A-Z]{2})/);
      this.ruleForm.backup_max_size_value = backup_file_max_size_parts[1];
      this.ruleForm.backup_max_size_unit = backup_file_max_size_parts[2];
    },

    viewBackup(data){
      this.copy(data);
      this.ruleForm.database = data.database;
      this.ruleForm.stable = data.stable;
      this.dialogTitle = `${this.$t('taosuser.backupPlan')}`;
      this.ruleForm.created_at = data.created_at;
      this.viewOnly = true;
    },
    parseBackup(data) {
      let targetData = {};
      targetData.id = data.id;
      targetData["database"] = data.from.split("/").at(-1);
      let params_start = targetData["database"].indexOf("?");
      if (params_start > 0) {
        targetData["database"] = targetData["database"].substring(0, params_start);
      }

      targetData.status = data.status;
      targetData.stable = data.from_expand.params.stable;
      targetData.upcoming = data.trigger.upcoming;

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
      targetData.created_at = parsinginZone(data.created_at);
      return targetData;
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
            this.getRestoreTasks();
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
            this.getRestoreTasks();
          }
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    //切换状态
    switchOperation(val, data, tip) {
      if (val) {
        this.$confirm(
          this.$t(tip)
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
          this.$t(tip)
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
          try {
            if (this.currentId) {
              await editBackup(this.currentId, postData);
            } else {
              await addBackupData(postData);
            }
          } catch (err) {
            this.$error(err);
            return;
          }
          
          Message.success(this.$t('operateSucc'));
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
          "upcoming": this.ruleForm.upcoming, 
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
    
    parseRestore(data) {
      return {
        id: data.id,
        from_path: data.from_expand.path,
        from_point_start: data.from_expand.params.from,
        from_point_end: data.from_expand.params.to,
        to_database: data.to_expand.subject,
        status: data.status,
        created_at: data.created_at,
      };
    },

    async getRestoreTasks() {
      try {
        this.requestIng = true;
        let id = localStorage.getItem("local_clusterID");
        let result = await getBackupList(id, "restore");
        this.restoreList = result.map((item) => this.parseRestore(item));
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
    async getBackData() {
      try {
        this.requestIng = true;
        let id = localStorage.getItem("local_clusterID");
        let result = await getBackupList(id, "backup");
        this.topicList = result.map((item) => this.parseBackup(item));
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
      this.getRestoreTasks();
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
.cover-readonly {
  position: absolute; left:0; right:0; top:50px; bottom:0;z-index:10;
}
.w100 {
  width: 80px;
}
</style>
