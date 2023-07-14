<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button plain @click="showDialog" size="small" icon="el-icon-plus" :disabled='!isDisable'>{{ $t("add") }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="usersList" size="mini" v-loading="loading">
      <el-table-column :label="$t('userName')" prop="name"></el-table-column>
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
      <!-- <el-table-column :label="$t('taosuser.database')">
        <template slot-scope="scope" v-if="scope.row.super !== 1">
          <el-tooltip placement="right" effect="light" v-if="filterPrivileges(scope).length > 1">
            <ul slot="content">
              <li v-for="(item, index) in filterPrivileges(scope)" :key="index">
                <span>{{ item.name }}: {{ item.privileges }}</span>
              </li>
            </ul>
            <span>{{ filterPrivileges(scope)[0]['name'] }}: {{ filterPrivileges(scope)[0]['privileges'] }}
              <i class="el-icon-more-outline" :style="{ 'vertical-align': 'bottom' }"></i></span>
          </el-tooltip>
          <span v-if="filterPrivileges(scope).length == 1">{{ filterPrivileges(scope)[0]['name'] }}: {{
            filterPrivileges(scope)[0]['privileges'] }}</span>
        </template>
      </el-table-column> -->
      <!-- <el-table-column :label="$t('taosuser.topic')">
        <template slot-scope="scope" v-if="scope.row.super !== 1">
          <el-tooltip placement="right" effect="light" v-if="filterTopic(scope).length > 1">
            <ul slot="content">
              <li v-for="(item, index) in filterTopic(scope)" :key="index">
                <span>{{ item.name }}: {{ item.privileges }}</span>
              </li>
            </ul>
            <span>{{ filterTopic(scope)[0]['name'] }}: {{ filterTopic(scope)[0]['privileges'] }}
              <i class="el-icon-more-outline" :style="{ 'vertical-align': 'bottom' }"></i></span>
          </el-tooltip>
          <span v-if="filterTopic(scope).length == 1">{{ filterTopic(scope)[0]['name'] }}: {{
            filterTopic(scope)[0]['privileges'] }}</span>
        </template>
      </el-table-column> -->

      <!-- <el-table-column label="Permission">
        <template slot-scope="scope">
          <span>{{ scope | filterVal }}</span>
        </template>
      </el-table-column> -->
      <el-table-column :label="$t('taosuser.createtime')" prop="create_time"></el-table-column>

      <el-table-column :label="$t('taosuser.action')" width="150">
        <template slot-scope="scope">
          <el-switch :value="scope.row.enable == 1" :disabled="(scope.row.super === 1 || !currentUser.super)||!isDisable"
            @change="changeState(scope.row)" active-color="#13ce66" inactive-color="#6D7074">
          </el-switch>&nbsp;&nbsp;
          <el-button plain size="small" @click="edit(scope.row)" :disabled="(scope.row.super === 1 || !currentUser.super)||!isDisable"
            icon="el-icon-edit"></el-button>
          <el-button plain size="small" @click="del(scope.row)" :disabled="(scope.row.super === 1 || !currentUser.super)||!isDisable"
            icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination class="pagination" layout="total, prev, pager, next" :current-page.sync="currentPage"
      :page-size="pageSize" :hide-on-single-page="true" :total="total" @current-change="handlePageChange"></el-pagination>

    <el-dialog align="center" :title="$t('taosuser.adduser')" width="680px" :visible.sync="dialog">
      <AddUser @close="closeDialog" :status='dialog' v-if='dialog'></AddUser>
    </el-dialog>

    <el-dialog align="center" :title="$t('taosuser.edituser')" width="680px" :visible.sync="editDialog">
      <EditUser :user="this.editUser" @close="closeEditDialog"></EditUser>
    </el-dialog>

  </div>
</template>
<script>

import AddUser from "./components/AddUser";
import EditUser from "./components/EditUser";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
export default {
  components: {
    AddUser,
    EditUser
  },
  filters: {
    filterVal(val) {
      let res = [];
      if (val.row.enable === 1) {
        res.push("Enable");
      }
      if (val.row.super === 1) {
        res.push("Super");
      }
      if (val.row.sysinfo === 1) {
        res.push("SysInfo");
      }
      return res.join(", ");
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
      isDisable:localStorage.getItem('username')==='root',
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {
        user: "",
        pwd: "",
      },
      editDialog: false,
      rules: {
        user: [
          {
            message: this.$t('login.usernameTips'),
            trigger: "blur",
          },
        ],
        pwd: [
          {
            message: this.$t('login.passwordTips'),
            trigger: "blur",
          },
        ],
      },
      usersList: [],
      editUser: "",
      currentUser: {},
      loading: true
    };
  },
  created() {
    this.getUserData();
    this.getCurrentUser();
  },
  methods: {
     getCurrentUser() {
       this.$store.dispatch("app/getUserInfo").then((res) => {
         this.currentUser = res;
       });
    },
    closeDialog() {
      this.dialog = false
      this.getUserData();
    },
    closeEditDialog() {
      this.editDialog = false
      this.getUserData();
    },
    filterPrivileges(val) {
      let res = [];
      for (let k in val.row.privilege) {
        if (val.row.privilege[k].indexOf("subscribe") > -1) {
          continue;
        }
        res.push({ name: k, privileges: val.row.privilege[k].join(", ") });
      }
      return res;
    },
    filterTopic(val) {
      let res = [];
      for (let k in val.row.privilege) {
        if (val.row.privilege[k].indexOf("subscribe") > -1) {
          res.push({ name: k, privileges: val.row.privilege[k].join(", ") });
        }
      }
      return res;
    },
    showDialog() {
      this.dialog = true
      this.ruleForm.user = ''
      this.ruleForm.pwd = ''
    },

    handlePageChange() { },
    del(data) {
      this.$confirm(this.$t('isDel').replace('{isDelName}', data.name), this.$t('wraning'), {
        confirmButtonText: this.$t('confirm'),
        cancelButtonText: this.$t('cancel'),
        type: "warning",
      }).then(() => {
        sendSQLReq(`drop user \`${data.name}\``).then(res => {
          if (res.code == 0) {
            Message.success(this.$t('delSucc'))
            this.getUserData()
          }
        })
      });
    },
    edit(data) {
      this.$set(this, 'editUser', data.name);
      // this.editUser = data.name,
      this.editDialog = true
    },
    changeState(data) {
      let title = this.$t('isDisable').replace('{isDisableName}', data.name);
      let state = 0;
      if (data.enable == 0) {
        title = this.$t('isEnable').replace('{isDisableName}', data.name);
        state = 1;
      }
      this.$confirm(title, {
        confirmButtonText: this.$t('confirm'),
        cancelButtonText: this.$t('cancel'),
        type: "warning",
      }).then(() => {
        sendSQLReq(`alter user \`${data.name}\` enable ${state}`).then(res => {
          if (res.code == 0) {
            Message.success(this.$t('operateSucc'))
            this.getUserData()
          }
        })
      });
    },
    async getUserData() {
      try {
        this.loading = true
        let permissionMap = await sendSQLReq(`select * from information_schema.ins_users;`)
          .then((res) => {
            return res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });
          })
          .catch((err) => {
            return Promise.reject(err);
          });
        await sendSQLReq(`select * from information_schema.ins_user_privileges;`)
          .then((res) => {
            let privilegeMap = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });

            privilegeMap.forEach((data) => {
              let user = permissionMap.find((item) => item.name === data.user_name);

              if (user) {
                if (user.privilege === undefined) {
                  user.privilege = {};
                }
                if (user.privilege[data.db_name] === undefined) {
                  user.privilege[data.db_name] = [data.privilege];
                } else {
                  user.privilege[data.db_name].push(data.privilege);
                }
              }
            });
            let rootUserIndex = permissionMap.findIndex((item, k) => item.name === 'root');
            let rooUser = permissionMap[rootUserIndex];
            rooUser.name = "*" + rooUser.name;
            permissionMap.unshift(rooUser);
            permissionMap.splice(++rootUserIndex, 1);
            this.usersList = permissionMap;
            this.loading = false
          })
          .catch((err) => {
            this.loading = false
            return Promise.reject(err);
          });
      } catch (error) {
        this.loading = false
        console.log(error);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.line {
  width: 100%;
  height: 1px;
  background-color: #ebeef5;
  margin: 20px 0;
}
</style>