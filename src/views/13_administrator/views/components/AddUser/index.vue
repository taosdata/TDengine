<template>
  <div>
    <el-form
      :model="ruleForm"
      :rules="rules"
      ref="ruleForm"
      size="mini"
      label-width="auto"
      class="demo-ruleForm"
    >
      <el-form-item :label="$t('taosuser.username')" prop="user" required>
        <el-input v-model.trim="ruleForm.user"></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.password')" prop="pwd" required>
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
      >
        <ul>
          <li v-for="(item, index) in this.databaseList" :key="index">
            <label class="db-label">{{ item }}</label>
            <el-checkbox-group
              v-model="selectedDatabasePrivileges[item]"
              class="db-pri"
              @change="changePri($event)"
            >
              <el-checkbox label="Read">{{ $t('read') }}</el-checkbox>
              <el-checkbox label="Write">{{ $t('write') }}</el-checkbox>
              <!-- <el-checkbox label="All"></el-checkbox> -->
            </el-checkbox-group>
          </li>
        </ul>
      </el-form-item>

      <el-form-item
        :label="$t('taosuser.subscription')"
        v-if="this.topicList.length > 0"
      >
        <ul>
          <li v-for="(item, index) in this.topicList" :key="index">
            <label class="db-label">{{ item }}</label>
            <el-checkbox-group
              v-model="selectedTopicPrivileges[item]"
              class="topic-pri"
            >
              <el-checkbox label="Subscribe">{{ $t('subscribe') }}</el-checkbox>
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
        <el-button
          size="small"
          :disabled="confirmStatus"
          @click="createUser"
          class="w100"
          type="primary"
          >{{ $t("confirm") }}</el-button
        >
      </el-col>
    </el-row>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import { validPassword } from "@/utils/validate.js";
export default {
  props: {
    close: {
      type: Function,
      default: () => {},
    },
    status: {
      type: Boolean,
      default: false,
    },
  },
  async created() {
    await this.getDatabaseList();
    await this.getTopicList();
  },
  watch: {
    status: {
      deep: true,
      handler(val) {
        if (val) {
          this.ruleForm.user = "";
          this.ruleForm.pwd = "";
          this.selectedDatabasePrivileges = {};
          this.selectedTopicPrivileges = {};
        }
      },
    },
  },
  data() {
    var checkPassword = async (_, value, callback) => {
      this.err_msg = "";
      callback(
        validPassword(value) ? undefined : new Error(this.$t("passwordError"))
      );
    };
    return {
      ruleForm: {
        user: "",
        pwd: "",
      },
      rules: {
        user: [
          {
            required: true,
            message: this.$t("taosuser.username") + this.$t("requiredMessage"),
            // trigger: "blur",
          },
        ],
        pwd: [
          {
            required: true,
            message: this.$t("taosuser.password") + this.$t("requiredMessage"),
            // trigger: "blur",
          },
          { validator: checkPassword, trigger: "blur" }
        ],
      },
      databaseList: [],
      topicList: [],
      selectedDatabasePrivileges: {},
      selectedTopicPrivileges: {},
      confirmStatus: false,
    };
  },
  methods: {
    changePri() {},
    getDatabaseList() {
      try {
        sendSQLReq(`show databases;`)
          .then((res) => {
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
          })
          .catch((err) => {
            this.$emit("close");
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
      }
    },
    getTopicList() {
      try {
        sendSQLReq(`show topics;`)
          .then((res) => {
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
          })
          .catch((err) => {
            this.$emit("close");
            // err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
        // Message.error(error.desc);
      }
    },
    cancel() {
      this.$emit("close");
      return;
    },
    async grantPrivilege(privileges, dbName, userName) {
      return await sendSQLReq(
        `GRANT ${privileges} ON \`${dbName}\`.*  to \`${userName}\``
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
    createUser() {
      this.$refs["ruleForm"].validate((valid) => {
        if (valid) {
          try {
            return sendSQLReq(
              `CREATE USER 
              \`${this.ruleForm.user}\` PASS '${this.ruleForm.pwd}';`
            )
              .then((res) => {
                for (let key in this.selectedDatabasePrivileges) {
                  if (this.selectedDatabasePrivileges[key].length > 0) {
                    let privileges = this.selectedDatabasePrivileges[key];
                    privileges.forEach(async (item, index) => {
                      await this.grantPrivilege(item, key, this.ruleForm.user);
                    });
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
                Message.success(this.$t("users.createNewUserSucTip"));
                this.$emit("close");
              })
              .catch((err) => {
                // if (err && (err.code == "9728" || err.code == "848")) {
                //   Message.error(this.$t("users.createNewUseErrCause"));
                //   return;
                // }
                // Message.error(this.$t("users.createNewUseErrTip"));
                err && err.desc && Message.error(err.desc);
                this.$emit("close");
                return Promise.reject(err);
              });
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
  width: 100px;
  text-align: right;
}

.db-pri {
  display: inline-block;
  width: 215px;
  text-align: left;
}
.topic-pri {
  display: inline-block;
  width: 215px;
  text-align: left;
}
</style>