<template>
  <el-form
    :model="changeForm"
    :rules="rules"
    :status-icon="true"
    label-position="top"
    label-width="auto"
    size="small"
    ref="changeForm"
  >
    <!-- <el-form-item label="Usernmae" prop="Usernmae">
        <el-input v-model.trim="changeForm.Usernmae"></el-input>
      </el-form-item> -->
    <el-form-item v-if="needEmail" :label="$t('email')" prop="email">
      <el-input
        v-model.trim="changeForm.email"
        @keyup.enter.native="change"
        :placeholder="$t('email')"
      ></el-input>
    </el-form-item>
    <el-form-item :label="$t('oldPass')" prop="old_password">
      <el-input
        v-model.trim="changeForm.old_password"
        maxlength="16"
        :show-password="true"
        @keyup.enter.native="change"
        minlength="8"
        :placeholder="$t('oldPass')"
      ></el-input>
    </el-form-item>
    <el-form-item :label="$t('newPass')" prop="new_password">
      <el-popover trigger="click" placement="right-end">
        <ol
          style="list-style: unset; padding-left: 10px"
          v-html="$t('passwordTip')"
        ></ol>
        <el-input
          slot="reference"
          v-model.trim="changeForm.new_password"
          maxlength="16"
          :show-password="true"
          @keyup.enter.native="change"
          minlength="8"
          :placeholder="$t('newPass')"
        ></el-input>
      </el-popover>
    </el-form-item>
    <el-form-item :label="$t('confirmPass')" prop="confirm_password">
      <el-input
        v-model.trim="changeForm.confirm_password"
        maxlength="16"
        @keyup.enter.native="change"
        minlength="8"
        :show-password="true"
        :placeholder="$t('confirmPass')"
      ></el-input>
    </el-form-item>
    <p v-show="err_msg" class="errorText">{{ err_msg }}</p>
    <el-form-item label=" ">
      <el-button
        type="primary"
        :disabled="requestIng"
        :loading="requestIng"
        @click="change"
        >{{ $t("setting.saveChange") }}</el-button
      >
      <!-- <el-button plain @click="$emit('close')">{{ $t("cancel") }}</el-button> -->
    </el-form-item>
  </el-form>
</template>

<script>
import { validEmail, validPassword } from "@/utils/validate.js";
import { modifyUserPassword } from "@/api/gateway/console";
import { deleteCookieItem } from "@/utils/index";
import { decrypt } from "@/utils/index";
export default {
  props: {
    needEmail: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      changeForm: {
        // old_password: process.env.VUE_APP_PASSWORD,
        // new_password: process.env.VUE_APP_PASSWORD,
        // confirm_password: process.env.VUE_APP_PASSWORD,
        old_password: "",
        new_password: "",
        confirm_password: "",
      },
      err_msg: "",
      requestIng: false,
    };
  },
  computed: {
    rules() {
      const checkEmail = async (_, value, callback) => {
        if (!value || !validEmail(value)) {
          return callback(new Error(this.$t("emailError")));
        }
      };
      const validateOldPwd = async (_, value, callback) => {
        if (!value) {
          return callback(new Error(this.$t("oldPass") + this.$t("requiredMessage")));
        } else {
          if (value != decrypt(localStorage.getItem("pwd"))) {
            return callback(new Error(this.$t('oldPassError')));
          } else {
            return callback();
          }
        }
      };
      const checkPassword = async (_, value, callback) => {
        this.err_msg = "";
        callback(
          validPassword(value) ? undefined : new Error(this.$t("passwordError"))
        );
      };
      const cheakConfirmPassword = async (_, value, callback) => {
        this.err_msg = "";
        callback(
          value == this.changeForm.new_password
            ? undefined
            : new Error(this.$t("twoPassError"))
        );
      };
      return {
        email: [{ validator: checkEmail, trigger: "blur" }],
        old_password: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("oldPass") + this.$t("requiredMessage"),
          },
          { validator: validateOldPwd, trigger: "blur" },
        ],
        new_password: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("newPass") + this.$t("requiredMessage"),
          },
          { validator: checkPassword, trigger: "blur" },
        ],
        confirm_password: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("confirmPass") + this.$t("requiredMessage"),
          },
          { validator: cheakConfirmPassword, trigger: "blur" },
        ],
      }
    },
  },
  methods: {
    change() {
      if (this.requestIng) return;
      this.$refs["changeForm"].validate(async (valid) => {
        if (valid) {
          this.requestIng = true;
          let username = localStorage.getItem("username");
          await modifyUserPassword(
            username,
            `ALTER USER \`${username}\` PASS '${this.changeForm.new_password}'`
          )
            .then((res) => {
              if (res) {
                this.changeForm = {
                  old_password: "",
                  new_password: "",
                  confirm_password: "",
                };
                // this.$message.success(this.$t("login.changeSucc"));
                this.requestIng = false;
                localStorage.removeItem("username");
                localStorage.removeItem("pwd");
                deleteCookieItem();
                this.$alert(this.$t("changepwdtip"), this.$t("tips"), {
                  showCancelButton: false,
                  showConfirmButton: true,
                  confirmButtonText: this.$t("ok"),
                  closeOnClickModal: false,
                  showClose: false,
                  type: "success",
                }).then(() => {
                  this.$router.push({
                    path: "/login",
                  });
                });
              }
            })
            .catch((err) => {
              this.err_msg = err;
            })
            .finally(() => {
              this.requestIng = false;
            });
          // this.$store
          //   .dispatch("auth/change", this.changeForm)
          //   .then(() => {
          //     this.changeForm = {
          //       old_password: "",
          //       new_password: "",
          //       confirm_password: "",
          //     };
          //     if (this.needEmail) {
          //       this.changeForm.email = "";
          //     }
          //     this.$message.success(this.$t("login.changeSucc"));
          //     this.$emit("close");
          //   })
          //   .catch(err_msg => {
          //     this.err_msg = err_msg;
          //   })
          //   .finally(() => {
          //     this.requestIng = false;
          //   });
        }
      });
    },
  },
  mounted() {
    if (this.needEmail) {
      this.changeForm.email = "";
    }
  },
};
</script>

<style lang="scss" scoped>
.errorText {
  color: #ff4949;
  font-size: 12px;
  padding: 10px 0;
}

.loginBtn {
  width: 100%;
  margin-top: 20px;
  text-align: center;
}
</style>
