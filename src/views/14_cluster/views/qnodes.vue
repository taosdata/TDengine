<template>
  <div class="qnode-block">
    <div class="flexEnd">
      <el-button plain @click="add" size="small" icon="el-icon-plus" :disabled='!isDisable'>{{
        $t("add")
      }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="qnodesList" size="mini">
      <el-table-column
        :label="$t('taoscluster.endpoint')"
        prop="endpoint"
      ></el-table-column>
      <el-table-column
        :label="$t('taoscluster.createtime')"
        prop="create_time"
      ></el-table-column>

      <el-table-column :label="$t('taoscluster.action')" width="65">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
            :disabled='!isDisable'
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
      :title="$t('taoscluster.addqnodes')"
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
            @click="addQnodes"
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
      qnodesList: [],
      isDisable:localStorage.getItem('username')==='root',
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
    this.getAllQnodes();
  },
  methods: {
    handlePageChange() {},
    add() {
      this.dialog = true;
    },
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
        sendSQLReq(`drop qnode on dnode ${data.id};`).then((res) => {
          if (res.code == 0) {
            Message.success(this.$t('delSucc'));
            this.getAllQnodes();
          }
        });
      });
    },
    async addQnodes() {
      try {
        return await sendSQLReq(
          `create qnode  on dnode ${this.ruleForm.DNodes};`
        ).then((res) => {
          if (res.code == 0) {
            this.getAllQnodes();
            this.dialog = false;
          }
        });
      } catch (err) {
        err&&err.desc&Message.error(err.desc)
        return Promise.reject(err);
      }
    },
    async getAllQnodes() {
      try {
        return await sendSQLReq(
          `select * from information_schema.ins_qnodes;`
        ).then((res) => {
          this.qnodesList = res.data.map((data) => {
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
:v-deep {
  .el-form-item__content {
    display: flex;
  }
  .el-select {
    flex: 1;
    width: 100%;
  }
}
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
.qnode-block{
  max-height:150px;
  overflow: auto;
}
</style>