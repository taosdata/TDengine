<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button plain type="primary" @click="refresh" size="small" icon="el-icon-refresh" :disabled="refreshable || $COMMUNITY" style="font-size:14px;">
        {{ $t("refresh") }}
      </el-button>
      <el-tooltip
        placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
      >
        <template slot="content">
          <span v-html="$t('communityTip')"></span>
        </template>
        <el-button plain type="primary" @click="add" size="small" icon="el-icon-plus" style="font-size:14px;" :disabled="$COMMUNITY">{{ $t('taosuser.addreplication') }}</el-button>
      </el-tooltip>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column label="ID" width="60" prop="id">
        <template slot-scope="scope">
          <el-tooltip :content="String(scope.row.id)" placement="top-start">
            <span class="nowrap">{{ scope.row.id }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.fromdb')" prop="fromdb" width="120">
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.fromdb" placement="top-start">
            <span class="nowrap">{{ scope.row.fromdb }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.toinstance')" prop="hostport"  min-width="140">
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.hostport" placement="top-start">
            <copy-text :text="scope.row.hostport" isShowBtnText></copy-text>
          </el-tooltip>
        <!-- {{ scope.row.hostport }} -->
        </template>
      </el-table-column>
      <!-- <el-table-column :label="$t('taosuser.todb')" prop="db" show-overflow-tooltip></el-table-column> -->

      <el-table-column :label="$t('taosuser.status')" prop="status" width="80">
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.status" placement="top-start">
            <span class="nowrap">{{ handleDSStatus(scope.row.status) }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.reason')" prop="reason">
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.reason" placement="top-start">
            <span class="nowrap">{{ scope.row.reason }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.finishat')" prop="finished_at" show-overflow-tooltip>
        <span slot-scope="scope">{{ parsinginZone(scope.row.finished_at) }}</span>
      </el-table-column>
      <el-table-column :label="$t('taosuser.createat')" prop="created_at" show-overflow-tooltip>
        <span slot-scope="scope">{{ parsinginZone(scope.row.created_at) }}</span>
      </el-table-column>
      <el-table-column :label="$t('taosuser.operation')" width="110">
        <template slot-scope="scope">
          <el-switch :value="!['stopping','stopped'].includes(scope.row.status.toLowerCase())" active-color="#13ce66"
            inactive-color="#dcdfe6" @change="switchOperation($event, scope.row)" :disabled="$COMMUNITY"></el-switch>
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
          <el-button plain size="small" @click="del(scope.row, scope.$index)" icon="el-icon-delete" :disabled="$COMMUNITY"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination class="pagination" layout="total, prev, pager, next" :current-page.sync="currentPage"
      :page-size="pageSize" :hide-on-single-page="true" :total="total" @current-change="handlePageChange"></el-pagination>
    <el-dialog align="center" :title="$t('taosuser.addreplication')" width="600px" :visible.sync="dialog"
      @close="closeDialog" :destroy-on-close="true" :close-on-click-modal="false">
      <el-form :model="ruleForm" :rules="rules" ref="ruleForm" size="mini" label-width="auto" class="demo-ruleForm">
        <el-form-item prop="source" required>
          <!-- <el-input v-model.trim="ruleForm.source"></el-input> -->
          <template slot="label">
            {{ $t('taosuser.fromsource') }}
          </template>
          <el-select v-model="ruleForm.source" :placeholder="$t('pleaseSelect')">
            <el-option v-for="db in dblist" :key="db['node-key']" :label="db.name" :value="db.name"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="target" required>
          <template slot="label">
            {{ $t('taosuser.targetdsn') }}
            <el-tooltip effect="light" placement="top">
              <span slot="content" v-html="$t('datasource.replicationTargetInfo')"></span>
              <i class="el-icon-info"></i>
            </el-tooltip>
          </template>
          <el-input v-model.trim="ruleForm.target" placeholder="taos://192.168.0.1:6030/db2"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">
            {{ $t("cancel") }}
          </el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button size="small" :disabled="confirmStatus" @click="addReplication" class="w100" type="primary" :loading="requesting">{{
            $t("confirm") }}</el-button>
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script>
import { Message } from "element-ui";
import CopyText from '@/components/CopyText.vue'
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
import {
  getReplicationList,
  addReplicationData,
} from "@/api/explorer/replication";
import _ from "lodash";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import taosbenchmarkVue from "@/utils/config/mdx/taosbenchmark.vue";
import { parsinginZone } from '@/utils';
import { replicationMockData } from '@/const'
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
      parsinginZone,
      requesting: false,
    };
  },
  props: {
    isLessThen3_3_3_0: {
      type: Boolean,
    },
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
    fromUrl() {
      let native_url = localStorage.getItem("native_url")
      let base_url = native_url || localStorage.getItem("base_url")
      let splitArr = base_url.split('//')
      let url = splitArr[0] + "//" + splitArr[1]
      const type = this.isLessThen3_3_3_0 ? 'tmq' : 'sync';
      return (
        splitArr[0].startsWith('taos')
          ? type + ":" + "//" + splitArr[1]
          : type + "+" + url 
      );
    },
  },
  methods: {
    handlePageChange() { },
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
        this.$t("replication.backupDel").replace("{id}", data.id),
        this.$t("warning"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          await excuteDel(data.id).then((res) => {
            if (res && Object.hasOwnProperty.call(res, "id")) {
              Message({
                type: "success",
                message: this.$t('delSucc'),
              });
              this.getReplication();
            } else {
              Message({
                type: 'error',
                message: res.message
              })
            }
            });
        });
    },
    refresh() {
      this.refreshable = true;
      this.getReplication();
    },
    async addReplication() {
      try {
        this.requesting = true;
        let id = localStorage.getItem("local_clusterID");
        let params = {
          labels: [
            "type::replication",
            `cluster-id::${localStorage.getItem("local_clusterID")}`,
          ],
          to: `${this.ruleForm.target}`,
          from: `${this.fromUrl}/${this.ruleForm.source}?timeout=never`,
        };
        let res = await addReplicationData(id, params);
        console.log(res);
        this.requesting = false;
        if (_.has(res, "code") && _.has(res, "message") && res.code != 0) {
          this.$error(res.message);
          return;
        }
        Message.success(this.$t('createSucc'));
        this.requesting = false;
        this.getReplication();
        this.dialog = false;
      } catch (err) {
        this.requesting = false;
        console.error(err);
        this.$error(err?.message);
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
          if (res && Object.hasOwnProperty.call(res, "code")) {
            Message({
              type: 'error',
              message: res.message
            })
          } else {
            Message.success(this.$t('operateSucc'));
            this.getReplication();
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
            this.getReplication();
          }  
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    switchOperation(val, data) {
      console.log('val',val);
      this.$confirm(
        this.$t(val ? this.$t('replication.taskStart').replace("{id}", data.id) : this.$t('replication.taskStop').replace("{id}", data.id)),
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
            item["fromdb"] = _.get(item, "from_expand.subject");
            item["hostport"] = _.get(item,'to')
              // _.get(item, "to_expand.host") ||
              // "localhost" + (to_port ? `:${to_port}` : "");
            item["db"] = item.to_expand
              ? item.to_expand.subject
              : item["fromdb"];
            return item;
          });
        });
        this.$parent.$parent.$parent.taosxDisabled = false;
      } catch (error) {
        if (error.response.status == 404) {
          this.$parent.$parent.$parent.taosxDisabled = true
        }
        if (error.response.status === 500) {
          this.$parent.$parent.$parent.taosxDisabled = true
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
    handleDSStatus(value) {
      return this.$t('statuses.' + value);
    },
  },
  created() {
    if (this.$COMMUNITY) {
      this.topicList = replicationMockData
    } else {
      this.getDatabases();
      this.getReplication();
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
