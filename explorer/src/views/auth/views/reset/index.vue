<template>
  <el-form :model="resetPass" :rules="rules" :status-icon="true">
    <el-form-item :label="$t('newPass')" prop="password">
      <el-popover trigger="hover" popper-class="password-tip" placement="right">
        <div v-html="$t('passwordTip')"></div>
        <el-input
          slot="reference"
          v-model.trim="resetPass.password"
          @keyup.enter.native="updatePass"
          minlength="8"
          show-password
          maxlength="16"
        ></el-input>
      </el-popover>
    </el-form-item>
    <el-form-item :label="$t('confirmPass')" prop="confirm">
      <el-input v-model.trim="resetPass.confirm" minlength="8" show-password maxlength="16" @keyup.enter.native="updatePass"></el-input>
    </el-form-item>
    <p class="errorText">{{ errorText }}</p>
    <el-form-item label=" ">
      <section class="login-block">
        <el-button class="loginBtn" @click="updatePass" type="primary">{{ $t("save") }}</el-button>
      </section>
    </el-form-item>
  </el-form>
</template>

<script>
  import { updatePassword } from "@/api/auth";
  import { validPassword } from "@/utils/validate.js";
  export default {
    data() {
      var checkPassword = async (_, value, callback) => {
        if (!validPassword(value)) {
          return callback(new Error(this.$t("passwordError")));
        }
      };
      let cheakConfirmPassword = async (_, value, callback) => {
        if (value != this.resetPass.password || !value) return callback(new Error(this.$t("twoPassError")));
      };
      return {
        requestIng: false,
        resetPass: {
          password: process.env.password,
          confirm: process.env.password,
        },
        rules: {
          password: [{ validator: checkPassword, trigger: "blur" }],
          confirm: [{ validator: cheakConfirmPassword, trigger: "blur" }],
        },
        errorText: "",
      };
    },
    methods: {
      async updatePass() {
        if (this.requestIng) return;
        this.requestIng = true;
        // TODO 添加重定向
        this.resetPass.code = this.$route.query.code;
        let status = await updatePassword(this.resetPass)
          .then(() => {
            this.$message.success(this.$t("login.changeSucc"));
          })
          .catch(err => (this.errorText = err.message));
        if (!status) {
          this.$router.push("/auth/login");
        }
        this.requestIng = false;
      },
    },
  };
</script>

<style></style>
