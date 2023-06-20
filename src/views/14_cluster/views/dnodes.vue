<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="add"
        size="small"
        icon="el-icon-plus"
        :disabled="!isDisable"
        >{{ $t("add") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="dnodesList" size="mini">
      <el-table-column
        width="400"
        :label="$t('taoscluster.endpoint')"
        prop="endpoint"
      ></el-table-column>
      <el-table-column
        :label="$t('taoscluster.vnodes')"
        prop="vnodes"
      ></el-table-column>
      <el-table-column
        :label="$t('taoscluster.supportvnodes')"
        prop="support_vnodes"
      ></el-table-column>
      <el-table-column
        :label="$t('taoscluster.status')"
        prop="status"
      ></el-table-column>
      <el-table-column
        :label="$t('taoscluster.createtime')"
        prop="create_time"
        width="200"
      ></el-table-column>
      <!-- <el-table-column :label="$t('topic.note')" prop="note"></el-table-column> -->

      <el-table-column :label="$t('taoscluster.action')" width="65">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
            :disabled="!isDisable"
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
      :title="$t('taoscluster.adddnodes')"
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
        <el-form-item
          :label="$t('taoscluster.endpoint')"
          prop="endpoint"
          required
        >
          <el-input v-model.trim="ruleForm.endpoint" ref="endinput"></el-input>
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
            @click="addDnodes"
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
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import mix from "./mix";
export default {
  mixins: [mix],
  data() {
    return {
      isDisable: localStorage.getItem("username") === "root",
      dnodesList: [],
      rules: {
        endpoint: [
          {
            required: true,
            message: this.$t('taoscluster.endpointRequired')
          }
        ]
      }
    };
  },
  created() {
    this.getAllDnodes();
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm(
        this.$t("isDel").replace("{isDelName}", data.endpoint),
        this.$t("wraning"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      ).then(() => {
        try {
          sendSQLReq(`drop dnode ${data.id}`).then((res) => {
            if (res.code == 0) {
              Message.success(this.$t("delSucc"));
              this.getAllDnodes();
            }
          }).catch(err=>{
            err.desc && Message.error(err.desc);
          });
        } catch (error) {
          console.log(error, "删除");
        }
      });
    },
    add() {
      this.dialog = true;
      this.ruleForm.endpoint = "";
      this.$nextTick(() => {
        this.$refs.endinput.blur();
      });
    },
    async addDnodes() {
      try {

        return await sendSQLReq(
          `create dnode \`${this.ruleForm.endpoint}\`;`
        ).then((res) => {
          if (res.code == 0) {
            this.getAllDnodes();
            this.dialog = false;

          }
        });
      } catch (err) { 
        err && err.desc & Message.error(err.desc);

        return Promise.reject(err);
      }
    },
    async getAllDnodes() {
      try {
        return await sendSQLReq(
          `select * from information_schema.ins_dnodes;`
        ).then((res) => {
          this.dnodesList= res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          this.$emit("sendData", this.dnodesList);
        });
      } catch (error) {
        console.log(error);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.flexEnd {
  position: absolute;
  top: 15px;
  z-index: 9999;
  right: 10px;
  .el-button {
    border: none;
    background: transparent;
  }
}

.dnode-block {
  max-height: 150px;

  overflow: auto;
}
</style>