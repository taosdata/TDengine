<template>
  <div>
    <el-form :model="ruleForm" :rules="rules" ref="ruleForm" size="mini" label-width="auto" class="demo-ruleForm">
      <el-form-item label="User Name" prop="user" required>
        <el-input v-model.trim="ruleForm.user"></el-input>
      </el-form-item>
      <el-form-item label="Password" prop="pwd" required>
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
        <el-button size="small" :disabled="confirmStatus" @click="createUser" class="w100" type="primary">{{ $t("confirm")
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
    close: {
      type: Function,
      default: () => { },
    },
  },
  created() {
    this.getDatabaseList();
  },
  data() {
    return {
      ruleForm: {
        user: "",
        pwd: ""
      },
      rules: {
        user: [
          { required: true, message: "Please input user name", trigger: "blur" }
        ],
        pwd: [
          { required: true, message: "Please input password", trigger: "blur" }
        ]
      },
      databaseList: [],
      selectedDatabasePrivileges: {},
      confirmStatus: false
    };
  },
  methods: {
    changePri() {
      console.log()
      console.log(this.selectedDatabasePrivileges);
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
            console.log(this.databaseList);
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
    cancel() {
      this.$emit("close");
      return;
    },
    async grantPrivilege(privileges, dbName, userName) {
      return await sendSQLReq(
        `GRANT ${privileges} ON ${dbName}.*  to ${userName}`
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
    createUser() {
      this.$refs["ruleForm"].validate((valid) => {
        if (valid) {
          try {
            return sendSQLReq(
              `CREATE USER ${this.ruleForm.user} PASS '${this.ruleForm.pwd}';`
            )
              .then((res) => {
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
              })
              .catch((err) => {
                this.$emit("close")
                err.desc && Message.error(err.desc);
                return Promise.reject(err);
              });
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