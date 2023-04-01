<template>
  <div class="data-source">
    <div class="flexEnd">
      <el-button
        plain
        @click="dialog = true"
        size="small"
        icon="el-icon-plus"
        >{{ $t("taospi.addpi") }}</el-button
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
      :title="$t('taospi.addnewpi')"
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
        <el-form-item
          :label="$t('taospi.UpdateInterval')"
          prop="UpdateInterval"
        >
          <el-input-number
            v-model="ruleForm.UpdateInterval"
          ></el-input-number>
        </el-form-item>
        <el-form-item
          :label="$t('taospi.PIServerName')"
          required
          prop="PIServerName"
        >
          <el-input v-model="ruleForm.PIServerName"></el-input>
        </el-form-item>
        <el-form-item :label="$t('taospi.PISystemName')" prop="PISystemName">
          <el-input v-model="ruleForm.PISystemName"></el-input>
        </el-form-item>
        <el-form-item
          required
          :label="$t('taospi.AFDatabaseName')"
          prop="AFDatabaseName"
        >
          <el-input v-model="ruleForm.AFDatabaseName"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('taospi.PIDataPipesInstances')"
          prop="PIDataPipesInstances"
        >
          <el-input-number
            v-model="ruleForm.PIDataPipesInstances"
          ></el-input-number>
        </el-form-item>
        <el-form-item
          :label="$t('taospi.AFDataPipesInstances')"
          prop="AFDataPipesInstances"
        >
          <el-input-number
            v-model="ruleForm.AFDataPipesInstances"
          ></el-input-number>
        </el-form-item>
        <el-form-item
          :label="$t('taospi.MaxBackfillRangeDays')"
          prop="MaxBackfillRangeDays"
        >
          <el-input-number
            v-model="ruleForm.MaxBackfillRangeDays"
          ></el-input-number>
        </el-form-item>
        <el-form-item :label="$t('taospi.TaosXEnabled')" prop="TaosXEnabled">
          <el-switch
            v-model="ruleForm.TaosXEnabled"
            active-color="#13ce66"
          >
          </el-switch>
        </el-form-item>
        <el-form-item :label="$t('taospi.MaxWaitLen')" prop="MaxWaitLen">
          <el-input-number
            v-model="ruleForm.MaxWaitLen"
          ></el-input-number>
        </el-form-item>
        <el-form-item :label="$t('taospi.IPCStream')" prop="IPCStream" required>
          <el-input v-model="ruleForm.IPCStream"></el-input>
        </el-form-item>
        <el-form-item :label="$t('taospi.SQLAPI')" prop="SQLAPI" required>
          <el-input v-model="ruleForm.SQLAPI" type="number"></el-input>
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
            @click="handleAdd"
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
import { getPI } from "@/api/explorer/datain";
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
      if(!this.ruleForm.IPCStream){
        return true
      }
      if(!this.ruleForm.SQLAPI){
        return true
      }
      return false;
    },
  },
  data() {
    return {
      dbsource: null,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {

        UpdateInterval: 10000,
        PIServerName: "",
        PISystemName: "",
        AFDatabaseName: "",
        PIDataPipesInstances: 1,
        AFDataPipesInstances: 1,
        MaxBackfillRangeDays: 1,
        TaosXEnabled: true,
        MaxWaitLen: 1000,
        IPCStream: "",
        SQLAPI: 8080,
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
        cancelButtonText: "Cancle",
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
            err.desc && Message.error(err.desc);
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
        this.$parent.dbsource = editDdata;
        this.$parent.toggleComponent("ui",dbname);
      }

      // this.$router.push({
      //   path: `/dataIn/source/${data.data_source_name}`
      // });
    },
    handleAdd() {
      console.log('获取参数',this.ruleForm);
      this.$parent.toggleComponent("ui");
    },
    async getList() {
      try {
        let id = localStorage.getItem("local_clusterID");
        await getPI(id).then((res) => {
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
        err.desc && Message.error(err.desc);
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
        err.desc && Message.error(err.desc);
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
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getList();
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
</style>
