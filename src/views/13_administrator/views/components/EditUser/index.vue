<template>
  <div v-loading="loading">
    <el-form
      :model="ruleForm"
      :rules="rules"
      ref="ruleForm"
      size="mini"
      label-width="auto"
      class="demo-ruleForm"
    >
      <el-form-item :label="$t('taosuser.username')" prop="user" required>
        <el-input v-model.trim="ruleForm.user" disabled></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.password')" prop="pwd">
        <!-- <el-input v-model.trim="ruleForm.pwd" type="password"></el-input> -->
        <el-popover trigger="click" placement="right-end">
          <ol
            style="list-style: unset; padding-left: 10px"
            v-html="$t('passwordTip')"
          ></ol>
          <el-input
            slot="reference"
            clear
            v-model.trim="ruleForm.pwd"
            maxlength="16"
            :show-password="true"
            minlength="8"
          ></el-input>
        </el-popover>
      </el-form-item>
      <div class="line"></div>

      <el-form-item
        :label="$t('taosuser.database')"
        v-if="this.databaseList.length > 0"
        class="database-item"
      >
      <el-tooltip
        placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
      >
        <template slot="content">
          <span v-html="$t('communityTip')"></span>
        </template>
        <ul>
          <li v-for="(item) in this.databaseList" :key="item">
            <label class="db-label">{{ item }}</label>
            <el-checkbox-group
              v-model="selectedDatabasePrivileges[item]"
              class="db-pri"
              @change="changePri($event)"
            >
              <el-checkbox :disabled="$COMMUNITY" label="Read">{{ $t('read') }}</el-checkbox>
              <el-checkbox :disabled="$COMMUNITY" label="Write">{{ $t('write') }}</el-checkbox>
              <!-- <el-checkbox label="All"></el-checkbox> -->
            </el-checkbox-group>
          </li>
        </ul>
      </el-tooltip>
      </el-form-item>
      <el-form-item
        :label="$t('taosuser.subscription')"
        v-if="this.topicList.length > 0"
        class="database-item"
      >
      <el-tooltip
        placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
      >
        <template slot="content">
          <span v-html="$t('communityTip')"></span>
        </template>
        <ul>
          <li v-for="(item) in this.topicList" :key="item">
            <label class="db-label">{{ item }}</label>
            <el-checkbox-group
              v-model="selectedTopicPrivileges[item]"
              class="db-pri"
            >
              <el-checkbox :disabled="$COMMUNITY" label="Subscribe">{{ $t('subscribe') }}</el-checkbox>
            </el-checkbox-group>
          </li>
        </ul>
      </el-tooltip>
      </el-form-item>
    </el-form>

    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button size="small" @click="cancel" class="w100">{{
          $t("cancel")
        }}</el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button
          size="small"
          :disabled="confirmStatus"
          @click="editUser"
          class="w100"
          type="primary"
          >{{ $t("confirm") }}</el-button
        >
      </el-col>
    </el-row>
  </div>
</template>

<script>
import { sendSQLReq, modifyUserPassword } from "@/api/gateway/console";
import { Message } from "element-ui";
// import { validPassword } from "@/utils/validate.js";

export default {
  props: {
    user: {
      type: String,
      default: "",
    },
    close: {
      type: Function,
      default: () => {},
    },
    status: {
      type: Boolean,
      default: false,
    }
  },
  watch: {
    status: {
      async handler(val) {
        if (val) {
          this.loading = true;
          this.ruleForm.user = this.user;
          this.databaseList = [];
          this.selectedDatabasePrivileges = {};
          this.selectedTopicPrivileges = {};
          this.topicList = [];
          await this.getDatabaseList();
          await this.getTopicList();
          await this.getUserPrivileges();
          await this.getUserTopics();
        }
      },
      immediate: true,
    }
  },
  created() {},
  data() {
    function validPassword(password) {
      if (password) {
        return (
          /^(?![A-Za-z]+$)(?![A-Z0-9]+$)(?![a-z0-9]+$)(?![a-z\W]+$)(?![A-Z\W]+$)(?![0-9\W]+$)[a-zA-Z0-9_\W]{8,16}$/.test(
            password
          )
        );
      }else{
        return true
      }
    }
    var checkPassword = async (_, value, callback) => {
      this.err_msg = "";
      callback(
        validPassword(value) ? undefined : new Error(this.$t("passwordError"))
      );
    };
    return {
      ruleForm: {
        user: this.user,
        pwd: "",
      },
      rules: {
        user: [
          {
            required: true,
            message: "Please input user name",
            trigger: "blur",
          },
        ],
        pwd: [
          {
            required: false,
            message: "Please input password",
            trigger: "blur",
          },
          { validator: checkPassword, trigger: "blur" },
        ],
      },
      databaseList: [],
      topicList: [],
      prevDatabasePrivileges: {},
      selectedDatabasePrivileges: {},
      prevTopicPrivileges: {},
      selectedTopicPrivileges: {},
      confirmStatus: false,
      loading: false
    };
  },
  methods: {
    changePri() {},
    async fetchData() {
      await this.getDatabaseList();
      await this.getTopicList();
      await this.getUserPrivileges();
      await this.getUserTopics();
    },
    async getDatabaseList() {
      try {
        let res = await sendSQLReq(`show databases;`)
        let databaseList = res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
        databaseList.forEach((item) => {
          if (
            ["performance_schema", "information_schema"].indexOf(
              item.name
            ) < 0
          ) {
            this.databaseList.push(item.name);
            this.$set(this.selectedDatabasePrivileges, item.name, []);
          }
        });
        console.log('1getDatabaseList()');
      } catch (error) {
        this.$emit("close")
        console.log(error);
      }
    },
    async getTopicList() {
      try {
        let res = await sendSQLReq(`show topics;`)
        let topicList = res.data.map((data) => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });
        topicList.forEach((item) => {
          this.topicList.push(item.topic_name);
          this.$set(this.selectedTopicPrivileges, item.topic_name, []);
        });
        console.log('2getTopicList()');
      } catch (error) {
        console.log(error);
        this.$emit("close");
        // this.$error(error.desc);
      }
    },
    async getUserPrivileges() {
      try {
        let res = await sendSQLReq(
          `select * from information_schema.ins_user_privileges where user_name = '${this.ruleForm.user}' and privilege<>'subscribe';`
        )
        let selectedDatabasePrivileges = {};
        res.data.map((data) => {
          if (this.selectedDatabasePrivileges[data[2]] === undefined) {
            let name = data[2];
            let pri = data[1].slice(0, 1).toUpperCase() + data[1].slice(1);
  
            this.$set(this.selectedDatabasePrivileges, name, [pri]);
            this.$set(this.prevDatabasePrivileges, name, [pri]);
          } else {
            let name = data[2];
            let pri = data[1].slice(0, 1).toUpperCase() + data[1].slice(1);
            this.selectedDatabasePrivileges[name].push(pri);
            this.$set(
              this.selectedDatabasePrivileges,
              data[2],
              this.selectedDatabasePrivileges[name]
            );
            this.$set(
              this.prevDatabasePrivileges,
              data[2],
              this.selectedDatabasePrivileges[name]
            );
          }
        });
        console.log('3getUserPrivileges()');
      } catch (error) {
        console.log(error);
        this.$emit("close");
      }
        
    },
    async getUserTopics() {
      try {
        let res = await sendSQLReq(
          `select * from information_schema.ins_user_privileges where user_name = '${this.ruleForm.user}' and privilege = 'subscribe';`
        )
        this.loading = false
        res.data.map((data) => {
          this.$set(this.selectedTopicPrivileges, data[2], ["Subscribe"]);
          this.prevTopicPrivileges = this.selectedTopicPrivileges;
        });
        console.log('4getUserTopics()');
      } catch (error) {
        console.log(error);
        this.$emit("close");
      } 
    },
    cancel() {
      this.$emit("close");
      return;
    },
    async grantPrivilege(privileges, dbName) {
      return await sendSQLReq(
        `GRANT ${privileges} ON \`${dbName}\`.*  to \`${this.user}\``
      )
        .then((res) => {
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close");
          return Promise.reject(err);
        });
    },
    async grantTopic(topicName, userName) {
      return await sendSQLReq(
        `GRANT subscribe ON \`${topicName}\` to \`${userName}\``
      )
        .then((res) => {
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close");
          return Promise.reject(err);
        });
    },
    async alterUser() {
      return await modifyUserPassword(
        this.user, 
        `alter USER \`${this.user}\` PASS '${this.ruleForm.pwd}';`
      )
        .then((res) => {
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close");
          return Promise.reject(err);
        });
    },
    async cancelPrivilege(privilege, dbName) {
      return await sendSQLReq(
        `REVOKE ${privilege} ON \`${dbName}\`.* FROM \`${this.user}\`;`
      )
        .then((res) => {
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close");
          return Promise.reject(err);
        });
    },
    async cancelTopic(topicName) {
      return await sendSQLReq(
        `REVOKE subscribe ON \`${topicName}\` FROM \`${this.user}\`;`
      )
        .then((res) => {
          return Promise.resolve(res);
        })
        .catch((err) => {
          this.$emit("close");
          return Promise.reject(err);
        });
    },
    editUser() {
      this.$refs["ruleForm"].validate(async (valid) => {
        if (valid) {
          try {
            if (this.ruleForm.pwd) {
              await this.alterUser();
            }
            for (let key in this.prevDatabasePrivileges) {
              let privileges = this.prevDatabasePrivileges[key];

              if (this.selectedDatabasePrivileges[key] === undefined) {
                privileges.forEach(async (item, index) => {
                  await this.cancelPrivilege(item, key);
                });
              } else {
                privileges.forEach(async (item, index) => {
                  if (
                    this.selectedDatabasePrivileges[key].indexOf(item) === -1
                  ) {
                    await this.cancelPrivilege(item, key);
                  }
                });
              }
            }
            for (let key in this.selectedDatabasePrivileges) {
              if (this.selectedDatabasePrivileges[key].length > 0) {
                let privileges = this.selectedDatabasePrivileges[key];
                privileges.forEach(async (item, index) => {
                  await this.grantPrivilege(item, key);
                });
              }
            }

            for (let key in this.prevTopicPrivileges) {
              if (this.selectedTopicPrivileges[key] === undefined) {
                await this.cancelTopic(key);
              } else {
                if (
                  this.selectedTopicPrivileges[key].indexOf("Subscribe") === -1
                ) {
                  await this.cancelTopic(key);
                }
              }
            }

            for (let key in this.selectedTopicPrivileges) {
              if (this.selectedTopicPrivileges[key].length > 0) {
                let privileges = this.selectedTopicPrivileges[key];
                privileges.forEach(async (item, index) => {
                  await this.grantTopic(key, this.ruleForm.user);
                });
              }
            }
            Message.success(this.$t("operateSucc"));
            this.$emit("close");
          } catch (error) {
            console.log(error);
          }
        } else {
          return false;
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.db-label {
  display: inline-block;
  margin-right: 30px;
  width: 240px;
  text-align: left;
}

.db-pri {
  display: inline-block;
  width: 215px;
  text-align: left;
}
.database-item {
  li {
    text-align: left;
    padding-left: 2px;
  }
  ::v-deep .el-form-item__content {
    padding-top: 5px;
  }
}
</style>
