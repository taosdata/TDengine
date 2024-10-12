<template>
  <div class="dnode-block" v-loading="loading">
    <div class="flexEnd">
      <el-button
        plain
        type="primary"
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="loading || $COMMUNITY"
        style="font-size: 14px"
        >{{ $t("refresh") }}</el-button
      >
      <el-button plain type="primary" @click="add" size="small" style="font-size: 14px" :disabled="$COMMUNITY">{{
        $t("taosuser.activationLicense")
      }}</el-button>
    </div>

    <!-- <el-table :data="tableData" :show-header="false" border>
      <el-table-column prop="header" label="表头"> </el-table-column>
      <el-table-column
        v-for="(item, index) in columns"
        :key="index"
        :prop="String(index)"
      >
      </el-table-column>
    </el-table> -->
    <p class="title">
      <span>{{ $t("topic.basicDatabaseFeatures") }}</span>
    </p>
    <el-descriptions class="margin-top" title="" :column="3">
      <el-descriptions-item :label="$t('topic.clusterId')" :labelStyle="style">
        <span>{{ clusterId }}</span>
      </el-descriptions-item>
      <el-descriptions-item
        v-for="item in licenseList"
        :key="item.key"
        :label="$INDUSTRY && item.key == 'version' ? $t('header.power') : $t(`topic.${item.key}`)"
        :labelStyle="style"
      >
        <span style="color: #333" v-if="item.key !== 'version'">
          {{
            ["expire_time","service_time"].includes(item.key) && item.value !== "unlimited"
              ? parsinginZone(item.value, "YYYY-MM-DD h:mm:ss")
              : item.value
          }}</span
        >
        <span style="color: #333" v-else>
          <span style="padding-left: 2px">{{ serverVersion }}</span>
          <!-- {{ item.value }} -->
        </span>
      </el-descriptions-item>
    </el-descriptions>
    <template v-if="!version_no_later_than_3230" >
      <p class="title">
        <span>{{ $t("topic.advancedDatabaseFeatures") }}</span>
      </p>
      <el-table style="margin-top: 20px" :data="advancedTableData" size="mini">
        <el-table-column :label="$t('topic.advancedFeatures')" prop="display_name"></el-table-column>
        <el-table-column :label="$t('topic.number')" prop="limits">
          <template slot-scope="scope">
            <span>{{
              formatLimits(scope.row.limits)
            }}</span>
          </template>
        </el-table-column>
        <!-- 占位 -->
        <el-table-column />
        <el-table-column
          :label="$t('topic.expire_time')"
          prop="expire"
        >
          <template slot-scope="scope">
            <span>{{ scope.row.expire == 'unlimited' ? 'unlimited' : expireTime(scope.row.expire) }}</span>
          </template>
        </el-table-column>
      </el-table>
    </template>
    <p class="title" v-if="getMetaShow('dataIn')">
      <span>{{ $t("topic.connectors") }}</span>
    </p>
    <el-table style="margin-top: 20px" :data="tableData" size="mini" v-if="getMetaShow('dataIn')">
      <el-table-column :label="$t('topic.type')" prop="type"></el-table-column>
      <el-table-column :label="$t('topic.tasks')" prop="number">
        <template slot-scope="scope">
          <span>{{
            scope.row.number == -1 ? "unlimited" : scope.row.number
          }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('topic.speed')" prop="speed">
        <template slot-scope="scope">
          <span>{{
            scope.row.speed == -1 ? "unlimited" : scope.row.speed
          }}</span>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('topic.expire_time')"
        prop="expire"
        v-if="version_no_later_than_3230"
      >
        <template slot-scope="scope">
          <span>{{ expireTime(scope.row.expire) }}</span>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('topic.expire_time')"
        prop="expireTime"
        v-if="!version_no_later_than_3230"
      >
        <template slot-scope="scope">
          <span>{{ scope.row.expireTime == 'unlimited' ? 'unlimited' : expireTime(scope.row.expireTime) }}</span>
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
      width="600px"
      :visible.sync="dialog"
      :destroy-on-close="true"
      :close-on-click-modal="false"
    >
      <div slot="title">
        <div class="activate-title">{{ $t("taosuser.activationLicense") }}</div>
        <span class="activate-tip">{{ $t("taosuser.activeTip") }}</span>
      </div>
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        :label-width="getlabelWidth"
        class="demo-ruleForm"
        label-position="left"
        @submit.native.prevent
      >
        <el-form-item :label="$t('taosuser.activeCode')" prop="active_code">
          <el-input v-model.trim="ruleForm.active_code" @keyup.enter.native="submit"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('taosuser.cActiveCode')"
          prop="c_active_code"
          v-if="version_no_later_than_3230"
        >
          <el-input v-model.trim="ruleForm.c_active_code" @keyup.enter.native="submit"></el-input>
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
import moment from "moment";
import { sendSQLReq } from "@/api/gateway/console";
import { activeLicence } from "@/api/explorer/licence";
import { parsinginZone, getBrowserLang } from "@/utils";
import LicenseMixin from "@/mixins/license"
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      loading: false,
      ruleForm: {
        active_code: "",
        c_active_code: "",
      },
      rules: {
        active_code: [
          {
            message: this.$t("dataIn.enterTip"),
          },
        ],
        c_active_code: [
          {
            message: this.$t("dataIn.enterTip"),
          },
        ],
      },
      licenseList: [],
      columns: [],
      tableData: [],
      advancedTableData: [],
      parsinginZone,
      version_no_later_than_3230: false,
      version_greater_than_3300: false,
      version_greater_than_3301: false,
    };
  },
  mixins: [LicenseMixin],
  computed: {
    style() {
      return {
        "font-size": "14px",
        color: "#4d6992",
        "min-width": this.$INDUSTRY && getBrowserLang() == 'en' ? "156px":  "110px",
        display: "inline-block",
        "text-align": "right",
      };
    },
    confirmStatus() {
      if (!this.ruleForm.active_code && !this.ruleForm.c_active_code) {
        return true;
      }
      return false;
    },
    getlabelWidth() {
      let lang = getBrowserLang();
      if (lang === "zh" && this.version_no_later_than_3230) {
        return "120px";
      }
      if (!this.version_no_later_than_3230) {
        return "auto"
      }
      return "240px";
    },
    clusterId() {
      return localStorage.getItem("local_clusterID") || "";
    },
    serverVersion() {
      return localStorage.getItem("serverVersion") || "";
    },
  },
  created() {
    this.getData();
    this.handlecActiveCodeShow();
  },
  methods: {
    handlecActiveCodeShow() {
      let version = localStorage.getItem("agent_version");
      let [a, b, c, d] = version.split(".");
      if (a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3)) {
        this.version_no_later_than_3230 = false;
        if (a > 3 || (a == 3 && b > 3) || (a == 3 && b == 3)){
          this.version_greater_than_3300 = true;
        }
        if (a > 3 || (a == 3 && b > 3) || (a == 3 && b >= 3 && c >0 ) || (a == 3 && b >= 3 && c >=0 && d > 0)){
          this.version_greater_than_3301 = true;
        }
      } else {
        this.version_no_later_than_3230 = true;
      }
    },
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + "?", "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancel",
        type: "warning",
      });
    },
    refresh() {
      this.loading = true;
      this.getData();
      this.getGrantsFull();
    },
    addUdf() {},
    async getData() {
      try {
        // let cols = [];
        // 不管是任何版本都show grants
        await sendSQLReq(`show grants;`).then((res) => {
          let array = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                // cols.push({ header: item[0], value: item[0] });
                return [item[0], data[index]];
              })
            );
          });
          let allLicence =
            array.length > 0
              ? Object.keys(array[0]).map((key) => {
                  return {
                    key: key,
                    value: array[0][key],
                  };
                })
              : [];
          this.licenseList = allLicence.filter(
            (item) => item.value.indexOf("{") == -1
          );
          if (this.version_no_later_than_3230) {
            this.tableData = allLicence
              .filter((item) => item.value.indexOf("{") == 0)
              .map((data) => {
                return JSON.parse(data.value);
              });
          }
        });
        if (!this.version_no_later_than_3230) {
          await sendSQLReq(`show grants full;`).then((res) => {
            let array = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });
 
            let allData = array
              .filter((item) => item.limits.indexOf("{") == 0)
              .map((data) => {
                return {
                  ...JSON.parse(data.limits),
                  type: data.display_name || data.grant_name,
                  grant: data.grant_name,
                  expire_time: data.expireTime,
                };
              })
            // 3.3.0.0 之前不显示 mysql、postgres、oracle
            this.tableData = allData
            .filter(v => !['mysql', 'postgres', 'oracle', '__future_datain__'].includes(v.grant));

            // 3.3.0.0 之后增加 mysql、postgres
            if (this.version_greater_than_3300) {
              this.tableData = allData
                .filter(v => !['oracle'].includes(v.grant));
            } 
            // 3.3.0.1 之后增加 oracle
            if (this.version_greater_than_3301){
              this.tableData = allData.filter(v => !['__future_datain__'].includes(v.grant));
            } 
            this.advancedTableData = array
              .filter((item) => item.limits.indexOf("{") == -1)
            console.log("this.tableData", this.tableData, this.advancedTableData);
          });
        }
        this.loading = false;
      } catch (error) {
        this.loading = false;
      }
    },
    add() {
      this.dialog = true;
    },
    async submit() {
      try {
        if (this.confirmStatus) return
        await activeLicence(this.ruleForm).then((res) => {
          if (res && res.code == 0) {
            this.$message.success(this.$t("operateSucc"));
            this.dialog = false;
            this.refresh();
            if (this.$INDUSTRY) {
              this.showLogoutConfirm();
            }
          } else {
            this.$error(res?.desc);
          }
        });
      } catch (error) {
        // this.$error(error);
        console.log('error:',error);
      }
    },
    expireTime(data) {
      if (this.version_no_later_than_3230) {
        return parsinginZone(Number(data) * 24 * 60 * 60 * 1000, "YYYY-MM-DD");
      } else {
        return parsinginZone(data, "YYYY-MM-DD hh:mm:ss");
      }
    },
    formatLimits(data) {
      if (data) {
        console.log('data',data);
        return data == 'unlimited' ? 'unlimited' : data.split('/')[1]
      } else {
        return 'n/a'
      }
    },
    logout() {
      localStorage.clear();

      this.$store.dispatch("app/logout");
      this.$router.push({
        path:'/login'
      })
      window.location.reload()
    },
    showLogoutConfirm() {
      this.$confirm(this.$t('taosuser.licenseSuccTip'),this.$t('tips'), {
        distinguishCancelAndClose: true,
        confirmButtonText: this.$t('signOut'),
        cancelButtonText: this.$t('cancel')
      })
        .then(() => {
          this.logout()
        })
        .catch(action => {
          console.log('cancel');
        });
    }
  },
};
</script>
<style lang="scss" scoped>
.dnode-block {
  margin-top: 10px;
}
::v-deep {
  .el-form-item__content {
    display: flex;
  }
  .el-select.el-select--mini {
    flex: 1;
  }

  th.el-descriptions-item__cell.el-descriptions-item__label.is-bordered-label {
    width: 80px;
  }
  td.el-descriptions-item__cell.el-descriptions-item__content {
    width: 200px;
  }
  .el-descriptions .el-descriptions-item__cell {
    padding: 12px 5px;
    border-bottom: 1px solid #dfe6ec;
  }
  .el-form-item--mini .el-form-item__label {
    word-break: break-word;
  }
  .title {
    background-color: #ecf8ff;
    border-left-color: #50bfff;
    color: #333;
    border-left-width: 5px;
    border-left-style: solid;
    border-radius: 4px;
    font-size: 16px;
    margin: 30px 0 10px 0;
    padding: 8px 16px;
  }
  .activate-title {
    line-height: 26px;
    font-weight: 500;
    font-size: 20px;
    color: #4d6992;
  }
  .activate-tip {
    color: #909399;
  }
}
</style>
