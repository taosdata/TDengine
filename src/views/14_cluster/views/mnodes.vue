<template>
  <div class="mnode-block">
    <div class="flexEnd">
      <el-button plain @click="add" size="small" icon="el-icon-plus" :disabled='!isDisable'>{{
        $t("add")
      }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="mnodesList" size="mini">
      <el-table-column
        :label="$t('taoscluster.endpoint')"
        prop="endpoint"
        width="400"
      ></el-table-column>
      <el-table-column :label="$t('taoscluster.role')" prop="role"></el-table-column>
      <el-table-column
        :label="$t('taoscluster.status')"
        prop="status"
      ></el-table-column>
      <el-table-column
        :label="$t('taoscluster.createtime')"
        prop="create_time"
        width="240"
      ></el-table-column>

      <el-table-column :label="$t('taoscluster.action')" width="65">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
            :disabled='!isDisable'
            v-if="scope.row.role!=='leader'"
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
      :title="$t('taoscluster.addmnodes')"
      width="600px"
      :visible.sync="dialog"
      @close='closeDialog'
      :destroy-on-close='true'
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        class="demo-ruleForm"
      >
        <!-- <el-form-item label="End Point" prop="endpoint" required>
          <el-input v-model.trim="ruleForm.endpoint"></el-input>
        </el-form-item> -->
        <el-form-item label="DNodes" prop="DNodes" required>
          <el-select
            v-model="ruleForm.DNodes"
            placeholder=""
            style="width:100%;"
          >
            <el-option
              v-for="item in dnodes"
              :key="item.id"
              :label="item.endpoint"
              :value="item.id"
            ></el-option>
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
            :disabled="confirmStatus"
            @click="addMnodes"
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
  props: {
    dnodes: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      isDisable:localStorage.getItem('username')==='root',
      mnodesList: [],
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.DNodes) {
        return true;
      }
      return false;
    },
  },
  created() {
    this.getAllMnodes();
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm(
        this.$t('isDel').replace('{isDelName}',data.endpoint),
        this.$t('wraning'),
        {
          confirmButtonText: this.$t('confirm'),
          cancelButtonText: this.$t('cancel'),
          type: "warning",
        }
      ).then(() => {
        sendSQLReq(`drop mnode on dnode ${data.id};`)
          .then((res) => {
            if (res.code == 0) {
              Message.success(this.$t('delSucc'));
              this.getAllMnodes();
            }
          })
          .catch((err) => {
            return Promise.reject(err);
          });
      });
    },
    async addMnodes() {
      try {
        return await sendSQLReq(
          `create mnode on dnode ${this.ruleForm.DNodes};`
        ).then((res) => {
          if (res.code == 0) {
            this.getAllMnodes();
            this.dialog = false;
          }
        });
      } catch (err) {
        err&&err.desc&Message.error(err.desc)
        return Promise.reject(err);
      }
    },
    async getAllMnodes() {
      try {
        return await sendSQLReq(
          `select * from information_schema.ins_mnodes;`
        ).then((res) => {
          this.mnodesList = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
        });
      } catch (error) {
        console.log(error);
      }
    },
  },
  watch: {
    dnodes: {
      deep: true,
      handler(val) {
        this.dnodes = val;
      },
    },
  },
};
</script>
<style lang="scss" scoped>
.flexEnd{
  position: absolute;
  top:15px;
  z-index: 9999;
  right: 10px;
  .el-button{
    border: none;
    background: transparent;
  }
}
.mnode-block{
  max-height:150px;
  overflow: auto;
}
</style>