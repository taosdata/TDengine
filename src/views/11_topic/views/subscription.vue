<template>
  <div>
    <div class="flexEnd">
      <el-button
        class="big-button"
        plain
        :disabled='localUser!=="root"'
        @click="addShareTopicUser"
        size="small"
        icon="el-icon-plus"
        >{{ $t("topic.addShareTopicUser") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" size="mini" :data="subscriptionList">
      <el-table-column
        :label="$t('topic.user_name')"
        prop="user_name"
      ></el-table-column>
      <el-table-column :label="$t('taosuser.action')" width="150">
        <template slot-scope="scope">
          <el-switch :value="scope.row.enable == 1" :disabled="scope.row.super === 1 || !currentUser.super"
            @change="changeState(scope.row)" active-color="#13ce66" inactive-color="#6D7074">
          </el-switch>         
        </template>
      </el-table-column>
      <!-- <el-table-column label="Token" prop="id"></el-table-column>
      <el-table-column :label="$t('topic.topic')" prop="id"></el-table-column>
      <el-table-column :label="$t('createTime')" prop="id"></el-table-column>
      <el-table-column
        :label="$t('topic.database')"
        prop="id"
      ></el-table-column> -->
      <!-- <el-table-column fixed="right" width="50">
        <template slot-scope="{ row }">
          <el-button
            size="mini"
            @click="del(row)"
            plain
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column> -->
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
    <el-dialog
      align="center"
      :title="$t('topic.add_new_user')"
      width="400px"
      :visible.sync="dialog"
      :destroy-on-close="true"
    >
      <el-form
        :model="ruleForm"
        ref="ruleForm"
        label-width="120px"
        class="demo-ruleForm"
        :rules="rules"
      >
        <el-form-item :label="$t('topic.user_name')" prop="user_name">
          <el-select v-model="ruleForm.user_name" style="width: 100%">
            <el-option
              v-for="item in userList"
              :key="item.name"
              :label="item.name"
              :value="item.name"
            ></el-option>
          </el-select>
        </el-form-item>
        <!-- <el-form-item :label="$t('topic.expire_time')" prop="expire_time">
          <el-date-picker
            v-model="ruleForm.expire_time"
            style="width: 100%"
            :picker-options="expireTimeOPtion"
            type="datetime"
          ></el-date-picker>
        </el-form-item> -->
        <el-form-item>
          <el-button
            type="primary"
            style="width: 100%; height: 32px; padding: 4px 20px"
            @click="submotForm('ruleForm')"
            >新增</el-button
          >
        </el-form-item>
      </el-form>
    </el-dialog>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
export default {
  props: {
    topicId: {
      type: String,
      default: "",
    },
  },
  data() {
    return {
      expireTimeOPtion: {
        disabledDate(time) {
          return time.getTime() < Date.now();
        },
      },
      localUser:localStorage.getItem('username'),
      subscriptionList: [],
      dialog: false,
      userList: [],
      currentPage: 1,
      pageSize: 10,
      total: 0,
      requestIng: false,
      ruleForm: {
        user_name: "",
        expire_time: "",
      },
      rules: {
        user_name: [{ 
          required: true,
          message: this.$t("topic.user_name_required") 
        }],
      },
      currentUser: {}
    };
  },
  mounted() {
    this.getUserData()
    this.getCurrentUser();
  },
  watch: {
    topicId: {
      deep: true,
      handler(val) {
        this.getUserData()
      },
    },
  },
  methods: {
    getCurrentUser() {
       this.$store.dispatch("app/getUserInfo").then((res) => {
         this.currentUser = res;
       });
    },

    async addUser() {
      try {
        if (this.topicId) {
          await sendSQLReq(
            `grant subscribe on ${this.topicId}.* to ${this.ruleForm.user_name};`
          ).then((res) => {
            if (res.rows) {
              Message.success(this.$t("operateSucc"));
              // this.getData();
              this.getUserData()
            }
          });
        } else {
          Message({
            type: "error",
            message: this.$t("topic.select_topic_tip"),
          });
        }
        this.dialog = false;
      } catch (error) {
        console.log(error);
      }
    },
    submotForm(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.addUser();
        } else {
          return false;
        }
      });
    },
    del() {},
    handlePageChange() {},
    changeState(data) {
      let title = this.$t('isDisable').replace('{isDisableName}', data.user_name);
      let state = 0;
      if (data.enable == 0) {
        title = this.$t('isEnable').replace('{isDisableName}', data.user_name);
        state = 1;
      }
      this.$confirm(title, this.$t('wraning'), {
        confirmButtonText: this.$t('confirm'),
        cancelButtonText: this.$t('cancel'),
        type: "warning",
      }).then(() => {       
        sendSQLReq(`revoke subscribe on ${this.topicId}.* from ${data.user_name}`).then(res => {
          if (res.code == 0) {
            Message.success(this.$t("operateSucc"))
            this.getUserData()
          }
        })
      });
    },
    async getUserData() {
      try {
        let usersRes = await sendSQLReq(`select * from information_schema.ins_users;`)
        let usersMap = usersRes.data.map((data) => {
          return Object.fromEntries(
            usersRes.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
        let res = await sendSQLReq(`select user_name from information_schema.ins_user_privileges where privilege in ('all', 'subscribe') and db_name in ('${this.topicId}', 'all');`)
        let privilegeMap = res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        }); 
        let permissionMap = privilegeMap.map((item) => {
          let user = usersMap.find((data) => data.name === item.user_name );
          item.enable = 1
          item.super = user.super
          return item
        })
        let noSubscriptionList = usersMap.filter((item) => {
          return privilegeMap.every((data) => data.user_name != item.name );     
        })
        let rootUserIndex = permissionMap.findIndex((item, k) => item.user_name === 'root');
        let rooUser = permissionMap[rootUserIndex];
        rooUser.user_name = "*" + rooUser.user_name;
        permissionMap.unshift(rooUser);
        permissionMap.splice(++rootUserIndex, 1);  
        this.subscriptionList = permissionMap;  
        this.userList = noSubscriptionList  
      } catch (error) {
        console.log(error);
      }
    },
    addShareTopicUser() {
      this.dialog = true
      this.ruleForm.user_name = ''
      this.getUserData()
    }
  },
};
</script>

<style style='scss'>
.el-picker-panel__footer .el-button--text.el-picker-panel__link-btn {
  display: none;
}
</style>
