<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="showDialog"
        size="small"
        icon="el-icon-plus"
        >{{ $t("add") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="usersList" size="mini">
      <el-table-column :label="$t('topic.name')" prop="name"></el-table-column>
      <!-- <el-table-column
        :label="$t('topic.super')"
        prop="super"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.enable')"
        prop="enable"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.sysinfo')"
        prop="sysinfo"
      ></el-table-column> -->
      <el-table-column label="Permission">
        <template slot-scope="scope">
          <span>{{ scope | filterVal }}</span>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('topic.create_time')"
        prop="create_time"
      ></el-table-column>

      <el-table-column label="Action" width="65">
        <template slot-scope="scope">
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
      title="Add New User"
      width="600px"
      :visible.sync="dialog"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        class="demo-ruleForm"
      >
        <el-form-item label="User Name" prop="user" required>
          <el-input v-model.trim="ruleForm.user"></el-input>
        </el-form-item>
        <el-form-item label="Password" prop="pwd" required>
          <el-input v-model.trim="ruleForm.pwd" type="password"></el-input>
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
            @click="addData"
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
export default {
  filters: {
    filterVal(val) {
      let res = "";
      if (val.row.enable === 1) {
        res += "Enable ";
      }
      if (val.row.super === 1) {
        res += "Super ";
      }
      if (val.row.sysinfo === 1) {
        res += "SysInfo";
      }
      return res.split(" ").join(",");
    },
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.user) {
        return true;
      }
      if (!this.ruleForm.pwd) {
        return true;
      }
      return false;
    },
  },
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {
        user: "",
        pwd: "",
      },
      rules: {
        user: [
          {
            message: "Please enter the user name",
            trigger: "blur",
          },
        ],
        pwd: [
          {
            message: "Please enter the password",
            trigger: "blur",
          },
        ],
      },
      usersList: [],
    };
  },
  created() {
    this.getUserDatas();
  },
  methods: {
    showDialog(){
      this.dialog=true
      this.ruleForm.user=''
      this.ruleForm.pwd=''
    },
    addData() {
      try {
        return sendSQLReq(
          `CREATE USER ${this.ruleForm.user}  PASS '${this.ruleForm.pwd}';`
        )
          .then((res) => {
            this.dialog = false;
            this.getUserDatas();
          })
          .catch((err) => {
            err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
      }
    },
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + "?", "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning",
      }).then(() => {
        sendSQLReq(`drop user ${data.name}`).then(res=>{
          if(res.code==0){
            Message.success('Deleted Successfully!')
            this.getUserDatas()
          }
        })
      });
    },
    async getUserDatas() {
      try {
        return await sendSQLReq(`select * from information_schema.ins_users;`)
          .then((res) => {
            this.usersList = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });
          })
          .catch((err) => {
            err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
</style>