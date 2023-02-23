<template>
  <div>
    <el-form :model="ruleForm" :rules="rules" ref="ruleForm" size="mini" label-width="auto" class="demo-ruleForm">
      <el-form-item label="User Name" prop="user" required>
        <el-input v-model.trim="ruleForm.user" disabled></el-input>
      </el-form-item>
      <el-form-item label="Password" prop="pwd">
        <el-input v-model.trim="ruleForm.pwd" type="password"></el-input>
      </el-form-item>
      <div class="line"></div>

      <el-form-item label="Privilege">
        <ul>
          <li v-for="(item, index) in this.databaseList" :key="index">
            <label class="db-label">{{ item }}</label>
            <el-checkbox-group v-model="selectedDatabasePrivileges[item]" class="db-pri" @change="changePri($event)">
              <el-checkbox label="Read"></el-checkbox>
              <el-checkbox label="Write"></el-checkbox>
              <el-checkbox label="All"></el-checkbox>
            </el-checkbox-group>
          </li>
        </ul>
      </el-form-item>
    </el-form>

    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button size="small" @click="cancel" class="w100">{{
          $t("cancel")
        }}</el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button size="small" :disabled="confirmStatus" @click="editUser" class="w100" type="primary">{{ $t("confirm")
        }}</el-button>
      </el-col>
    </el-row>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import { async } from "q";

export default {
  props: {
    user: {
      type: String,
      default: ""
    },
    close: {
      type: Function,
      default: () => { },
    },
  },
  created() {
    console.log("aaa");
    console.log(this.user);
    this.getDatabaseList();
    this.getUserPrivileges();
    console.log(this.selectedDatabasePrivileges)
  },
  data() {
    return {
      ruleForm: {
        user: this.user,
        pwd: ""
      },
      rules: {
        user: [
          { required: true, message: "Please input user name", trigger: "blur" }
        ],
        pwd: [
          { required: false, message: "Please input password", trigger: "blur" }
        ]
      },
      databaseList: [],
      selectedDatabasePrivileges: {},
      confirmStatus: false
    };
  },
  methods: {
    changePri() {
    },
    getDatabaseList() {
      try {
        sendSQLReq(
          `show databases;`
        )
          .then((res) => {
            let databaseList = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });
            databaseList.forEach((item) => {
              if (["performance_schema", "information_schema"].indexOf(item.name) < 0) {
                this.databaseList.push(item.name);
                this.$set(this.selectedDatabasePrivileges, item.name, []);
              }
            });
          })
          .catch((err) => {
            this.$emit("close")
            err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
        Message.error(error.desc);
      }
    },
    getUserPrivileges() {
      sendSQLReq(
        `select * from information_schema.ins_user_privileges where user_name = '${this.ruleForm.user}';`
      ).then((res) => {
        res.data.map((data) => {
          if (this.selectedDatabasePrivileges[data[2]] === undefined) {
            let name = data[2];
            let pri = data[1].slice(0, 1).toUpperCase() + data[1].slice(1);

            this.$set(this.selectedDatabasePrivileges, name, [pri]);
          } else {
            let name = data[2];
            let pri = data[1].slice(0, 1).toUpperCase() + data[1].slice(1);
            this.selectedDatabasePrivileges[name].push(pri);
            this.$set(this.selectedDatabasePrivileges, data[2], this.selectedDatabasePrivileges[name]);
          }
          console.log(this.selectedDatabasePrivileges);
        });
      })
        .catch((err) => {
          this.$emit("close")
          err.desc && Message.error(err.desc);
          return Promise.reject(err);
        });
    },
    cancel() {
      this.$emit("close");
      return;
    },
    async grantPrivilege(privileges, dbName) {
      return await sendSQLReq(
        `GRANT ${privileges} ON ${dbName}.*  to ${this.user}`
      ).then((res) => {
        console.log(res)
        return Promise.resolve(res);
      })
        .catch((err) => {
          this.$emit("close")
          err.desc && Message.error(err.desc);
          return Promise.reject(err);
        });
    },
    async alterUser() {
      return await sendSQLReq(
        `alter USER ${this.user} PASS '${this.ruleForm.pwd}';`
      )
        .then((res) => {
          console.log(res)
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close")
          err.desc && Message.error(err.desc);
          return Promise.reject(err);
        });
    },
    async cancelPrivilege() {
      return await sendSQLReq(
        `REVOKE all ON *.* FROM ${this.user};`
      )
        .then((res) => {
          console.log(res)
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close")
          err.desc && Message.error(err.desc);
          return Promise.reject(err);
        });
    },
    editUser() {
      this.$refs["ruleForm"].validate((valid) => {
        if (valid) {
          try {
            if (this.ruleForm.pwd) {
              this.alterUser();
            }
            this.cancelPrivilege();

            for (let key in this.selectedDatabasePrivileges) {
              if (this.selectedDatabasePrivileges[key].length > 0) {
                let privileges = this.selectedDatabasePrivileges[key];
                privileges.forEach((item, index) => {
                  this.grantPrivilege(item, key, this.ruleForm.user);
                });
              }
            }
            Message.success("Create user successfully");
            this.$emit("close")
          } catch (error) {
            console.log(error);
            Message.error(error.desc)
          }
        } else {
          console.log('error submit!!');
          return false;
        }
      });
    },
  }
};
</script>

<style lang="scss" scoped>
.db-label {
  display: inline-block;
  margin-right: 30px;
  width: 100px;
}

.db-pri {
  display: inline-block;
}
</style>