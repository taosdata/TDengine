<template>
  <el-form
    :hide-required-asterisk="true"
    :model="changeForm"
    :rules="rules"
    :status-icon="true"
    label-position="right"
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
      <el-popover trigger="click">
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
      <el-button plain @click="$emit('close')">{{ $t("cancel") }}</el-button>
    </el-form-item>
  </el-form>
</template>

<script>
import { validEmail, validPassword } from "@/utils/validate.js";
export default {
  props: {
    needEmail: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    var checkEmail = async (_, value, callback) => {
      if (!value || !validEmail(value)) {
        return callback(new Error(this.$t("emailError")));
      }
    };
    var checkPassword = async (_, value, callback) => {
      this.err_msg = "";
      if (!validPassword(value)) {
        return callback(new Error(this.$t("passwordError")));
      }
    };
    let cheakConfirmPassword = async (_, value, callback) => {
      this.err_msg = "";
      if (value != this.changeForm.new_password || !value)
        return callback(new Error(this.$t("twoPassError")));
    };
    return {
      changeForm: {
        old_password: process.env.VUE_APP_PASSWORD,
        new_password: process.env.VUE_APP_PASSWORD,
        confirm_password: process.env.VUE_APP_PASSWORD,
      },
      rules: {
        email: [{ validator: checkEmail, trigger: "blur" }],
        old_password: [{ validator: checkPassword, trigger: "blur" }],
        new_password: [{ validator: checkPassword, trigger: "blur" }],
        confirm_password: [
          { validator: cheakConfirmPassword, trigger: "blur" },
        ],
      },
      err_msg: "",
      requestIng: false,
    };
  },
  methods: {
    change() {
      if (this.requestIng) return;
      this.$refs["changeForm"].validate((valid) => {
        if (valid) {
          this.requestIng = true;
          this.$store
            .dispatch("auth/change", this.changeForm)
            .then(() => {
              this.changeForm = {
                old_password: "",
                new_password: "",
                confirm_password: "",
              };
              if (this.needEmail) {
                this.changeForm.email = "";
              }
              // this.$message.success(this.$t("login.changeSucc"));
              
              this.$emit("close");
            })
            .catch((err_msg) => {
              this.err_msg = err_msg;
            })
            .finally(() => {
              this.requestIng = false;
            });
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
