<template>
  <div class="data-source">
    <div class="flexEnd">
      <el-button
        plain
        @click="dialog = true"
        size="small"
        icon="el-icon-plus"
        >{{ $t("taosopc.addopc") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column
        :label="$t('taospi.name')"
        prop="localname"
      ></el-table-column>
      <el-table-column
        :label="$t('taospi.type')"
        prop="localtype"
      ></el-table-column>
      <el-table-column
        :label="$t('taospi.target')"
        prop="target"
      ></el-table-column>
      <el-table-column
        :label="$t('taospi.createat')"
        prop="created_at"
      ></el-table-column>
      <!-- <el-table-column label="Finished At" prop="finished_at"></el-table-column> -->

      <el-table-column :label="$t('taospi.status')" prop="status">
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
                :content="$t('taospi.excutestart')"
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
                :content="$t('taospi.excutestop')"
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
        :label="$t('taospi.operation')"
        width="100"
        class="action"
      >
        <template slot-scope="scope">
          <el-button
            type="primay"
            size="small"
            :disabled="scope.row.from_detail === undefined"
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
      :title="$t('taosopc.addopc')"
      width="400px"
      :visible.sync="dialog"
      @closed="closeDialog"
    >
      <el-form
        :model="ruleForm"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        label-position="left"
        class="demo-ruleForm"
      >
        <el-form-item :label="$t('taosopc.opc_type')" prop="opc_type">
          <el-select v-model="ruleForm.opc_type" placeholder="">
            <el-option label="opcua" value="opcua"></el-option>
            <el-option label="opcda" value="opcda"></el-option>
          </el-select>
        </el-form-item>
        <template v-if="ruleForm.opc_type === 'opcua'">
          <p>
            <span style="color: #4d6992; font-size: 24px">{{
              $t("taosopc.ua_config")
            }}</span>
          </p>
          <el-form-item
            :label="$t('taosopc.connect_timeout')"
            prop="connect_timeout"
            required
          >
            <el-input-number
              v-model="ruleForm.connect_timeout"
            ></el-input-number>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.endpoint')"
            prop="endpoint"
            required
          >
            <!-- <el-input-number v-model="ruleForm.endpoint"></el-input-number> -->
            <div style="margin-bottom: 10px">
              <el-input
                placeholder="127.0.0.1"
                v-model="ruleForm.endpoint.ip"
              ></el-input>
            </div>
            <div style="margin-bottom: 10px">
              <el-input
                placeholder="8080"
                v-model="ruleForm.endpoint.port"
              ></el-input>
            </div>
            <div style="margin-bottom: 10px">
              <el-input
                placeholder="/OPCUA/SimulationServer"
                v-model="ruleForm.endpoint.direct"
              ></el-input>
            </div>
            <el-button
              type="primary"
              style="width: 100%"
              @click="getNodesOrTags"
              >{{ $t("taosopc.searchnodes") }}</el-button
            >
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.request_timeout')"
            prop="request_timeout"
            required
          >
            <el-input v-model="ruleForm.request_timeout"></el-input>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.security_policy')"
            prop="security_policy"
            required
          >
            <el-select v-model="ruleForm.security_policy">
              <el-option
                v-for="item in policiesList"
                :key="item"
                :label="item"
                :value="item"
              >
              </el-option>
            </el-select>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.security_mode')"
            prop="security_mode"
            required
          >
            <el-select v-model="ruleForm.security_mode">
              <el-option
                v-for="item in modeList"
                :key="item"
                :label="item"
                :value="item"
              >
              </el-option>
            </el-select>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.certificate')"
            prop="certificate"
            :required="
              ruleForm.security_mode !== 'Nonde' ||
              ruleForm.security_policy !== 'None'
            "
          >
            <el-input v-model="ruleForm.certificate"></el-input>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.private_key')"
            prop="private_key"
            :required="
              ruleForm.security_mode !== 'Nonde' ||
              ruleForm.security_policy !== 'None'
            "
          >
            <el-input v-model="ruleForm.private_key"></el-input>
          </el-form-item>
          <el-form-item :label="$t('taosopc.auth_method')" prop="auth_method">
            <el-select v-model="ruleForm.auth_method">
              <el-option
                v-for="item in authMethodList"
                :key="item"
                :label="item"
                :value="item"
              >
              </el-option>
            </el-select>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.username')"
            prop="username"
            :required="ruleForm.auth_method === 'UserName'"
          >
            <el-input v-model="ruleForm.username"></el-input>
          </el-form-item>
          <el-form-item
            :label="$t('taosopc.password')"
            prop="password"
            :required="ruleForm.auth_method === 'UserName'"
          >
            <el-input v-model="ruleForm.password"></el-input>
          </el-form-item>
          <p>
            <span style="color: #4d6992; font-size: 24px">
              {{ $t("taosopc.collect_config") }}
            </span>
          </p>
          <el-form-item
            :label="$t('taosopc.interval')"
            prop="interval"
            required
          >
            <el-input v-model="ruleForm.interval"></el-input>
          </el-form-item>
          <el-form-item :label="$t('taosopc.nodes')" prop="nodes" required>
            <el-select v-model="ruleForm.nodes" multiple>
              <el-option
                v-for="item in uaCollectNodes"
                :key="item.id"
                :label="item.name"
                :value="item.id"
              >
              </el-option>
            </el-select>
          </el-form-item>
        </template>
        <template v-if="ruleForm.opc_type === 'opcda'">
          <p>
            <span style="color: #4d6992; font-size: 24px">{{
              $t("taosopc.da_config")
            }}</span>
          </p>
          <el-form-item
            :label="$t('taosopc.server')"
            prop="server"
            required
          >
            <el-input v-model="ruleForm.server"></el-input>
          </el-form-item>
          <el-form-item :label="$t('taosopc.nodes')" prop="nodes" required 
            class="da-server">
            <el-input v-model="ruleForm.nodes"></el-input>
            <el-button type="primary" @click="getNodesOrTags" style="height:32px;">{{
              $t("taosopc.searchtag")
            }}</el-button>
          </el-form-item>
          
          <p>
            <span style="color: #4d6992; font-size: 24px">
              {{ $t("taosopc.collect_config") }}
            </span>
          </p>
          <el-form-item
            :label="$t('taosopc.interval')"
            prop="interval"
            required
          >
            <el-input v-model="ruleForm.interval"></el-input>
          </el-form-item>
          <el-form-item :label="$t('taosopc.tags')" prop="tags" required>
            <el-select v-model="ruleForm.tags" multiple>
              <el-option
                v-for="item in tagsLists"
                :key="item.value_type"
                :label="item.tag"
                :value="item.value_type"
              >
              </el-option>
            </el-select>
          </el-form-item>
        </template>
        <p>
          <span style="color: #4d6992; font-size: 24px">
            {{ $t("taosopc.report_config") }}
          </span>
        </p>
        <el-form-item :label="$t('taosopc.remote')" prop="remote" required>
          <el-input v-model="ruleForm.remote"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('taosopc.concurrent')"
          prop="concurrent"
          required
        >
          <el-input v-model="ruleForm.concurrent"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('taosopc.batch_size')"
          prop="batch_size"
          required
        >
          <el-input v-model="ruleForm.batch_size"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('taosopc.batch_timeout')"
          prop="batch_timeout"
          required
        >
          <el-input v-model="ruleForm.batch_timeout"></el-input>
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
            @click="submit('ruleForm')"
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
import { getOPC, getUaAndDaData } from "@/api/explorer/datain";
import { excuteStart, excuteStop, excuteDel } from "@/api/explorer/common";
export default {
  name: "DataSource",
  props: {
    sourceList: {
      type: Array,
      default() {
        return [];
      },
    },
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.PIServerName) {
        return true;
      }
      if (!this.ruleForm.AFDatabaseName) {
        return true;
      }
      if (!this.ruleForm.IPCStream) {
        return true;
      }
      if (!this.ruleForm.SQLAPI) {
        return true;
      }
      return false;
    },
  },
  data() {
    return {
      loading: false,
      policiesList: ["None", "Basic128Rsa15", "Basic256", "Basic256Sha256"],
      modeList: ["None", "Sign", "SignAndEncrypt"],
      authMethodList: ["Certificate", "UserName", "Anonymous"],
      dbsource: null,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      tagsLists: [],
      uaCollectNodes: [],
      ruleForm: {
        opc_type: "opcua",
        endpoint: {
          ip: "",
          port: "",
          direct: "",
        },
        connect_timeout: "",
        request_timeout: "",
        security_policy: "",
        security_mode: "",
        certificate: "",
        private_key: "",
        auth_method: "",
        username: "",
        password: "",
        server: "",
        nodes: "",
        interval: "",
        tags: "",
      },
      topicList: [],
    };
  },
  methods: {
    handlePageChange() {},
    closeDialog() {
      this.$refs.ruleForm.resetFields();
      this.dialog = false;
    },
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + "?", "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancel",
        type: "warning",
      }).then(async () => {
        await excuteDel(data.id)
          .then(() => {
            Message({
              type: "success",
              message: "Deleted Successfully",
            });
            this.getList();
          })
          .catch((err) => {
            return Promise.reject(err);
          });
      });
    },
    edit(data) {
      if (data.from_detail) {
        let editDdata = [].concat(data.from_detail);
        let dbname =
          data.to_expand && data.to_expand.subject
            ? data.to_expand.subject
            : "";
        this.$parent.uidata = editDdata;
        this.$parent.toggleComponent("ui");
      }

      // this.$router.push({
      //   path: `/dataIn/source/${data.data_source_name}`
      // });
    },
    submit(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.loading = true;
          this.handleAdd();
        } else {
          return false;
        }
        this.loading = false;
      });
    },
    handleAdd() {
      this.$parent.toggleComponent("ui", this.ruleForm.type);
    },
    async getList() {
      try {
        let id = localStorage.getItem("local_clusterID");
        await getOPC(id).then((res) => {
          if (res) {
            this.topicList = res.map((item) => {
              item["localname"] = item.name ? item.name : "tmq+" + item.id;
              item["localtype"] = item.from_detail ? item.from_detail.name : "";
              item["target"] = item.to_expand ? item.to_expand.subject : "";
              return item;
            });
          }
        });
      } catch (err) {
        // err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    start(data, index) {
      try {
        this.$confirm(
          `Are you sure to start the ${data.name} task?`,
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(async () => {
          await excuteStart(data.id).then((res) => {
            this.getList();
          });
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    stop(data) {
      try {
        this.$confirm(
          `Are you sure to stop the ${data.name} task?`,
          this.$t("warning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        ).then(async () => {
          await excuteStop(data.id).then((res) => {
            this.getList();
          });
        });
      } catch (err) {
        return Promise.reject(err);
      }
    },
    async getNodesOrTags() {
      try {
        let params = null;
        if (this.ruleForm.opc_type === "opcua") {
          params = {
            from: `opc+ua://${this.ruleForm.endpoint.ip}:${this.ruleForm.endpoint.port}${this.ruleForm.endpoint.direct}`,
          };
        } else {
          params = {
            from: `opc+da://${this.ruleForm.server}?nodes=${this.ruleForm.nodes}`,
          };
        }

        await getUaAndDaData(params).then((res) => {
          if (this.ruleForm.opc_type === "opcua") {
            this.uaCollectNodes = res;
          } else {
            this.tagsLists = res;
          }
        });
      } catch (error) {
        console.log(error);
      }
    },
  },
  created() {
    this.getList();
    // this.getNodesOrTags();
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
:deep {
  .el-input-number__increase,
  .el-input-number__decrease {
    height: 26px;
    display: flex;
    justify-content: center;
    align-items: center;
  }
}
.el-form-item.da-server {
  display: flex;
  flex-wrap: nowrap;
  ::v-deep {
    .el-form-item__content {
      display: flex;
      flex-direction: column;
      .el-button {
        margin-top: 10px;
      }
    }
  }
}
</style>
