<template>
  <div>
    <el-form :hide-required-asterisk="true" :model="loginForm" :rules="rules" ref="loginForm" style="margin-top: 20px">
      <el-form-item :label="$t('email')" prop="email">
        <el-input @keyup.enter.native="login" v-model.trim="loginForm.email" :placeholder="$t('email')"></el-input>
      </el-form-item>
      <el-form-item :label="$t('password')" prop="password">
        <el-input
          v-model.trim="loginForm.password"
          @keyup.enter.native="login"
          maxlength="16"
          minlength="8"
          :show-password="true"
          :placeholder="$t('password')"
        ></el-input>
      </el-form-item>
      <p class="errorText" v-show="err_msg">{{ err_msg }}</p>
      <section class="other-btn">
        <el-checkbox v-model="remember">{{ $t("login.rememberMe") }}</el-checkbox>
        <router-link class="forgot-btn" :to="{ name: 'forgot', params: { email: loginForm.email } }">{{ $t("forgotPass") }}</router-link>
      </section>
      <el-form-item>
        <section class="login-block">
          <el-button type="primary" class="loginBtn" @keyup.enter.native="login" @click="login">{{ $t("log-in") }}</el-button>
        </section>
      </el-form-item>
    </el-form>
    <!-- <el-divider
      ><span style="font-weight:bold">{{ $t("or") }}</span></el-divider
    > -->
  </div>
</template>

<script>
  import { validEmail, validPassword } from "@/utils/validate.js";
  export default {
    data() {
      var checkEmail = async (_, value, callback) => {
        this.err_msg = "";
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
      this.accountKey = window.btoa("account");
      return {
        loginForm: {
          email: process.env.VUE_APP_EMAIL,
          password: process.env.VUE_APP_PASSWORD,
        },
        rules: {
          email: [{ validator: checkEmail, trigger: "blur" }],
          password: [{ validator: checkPassword, trigger: "blur" }],
        },
        err_msg: "",
        remember: localStorage.getItem("remmemberMe") ? true : false,
      };
    },
    created() {
      if (this.remember) {
        let account = localStorage.getItem(this.accountKey);
        if (account) {
          this.loginForm = JSON.parse(window.atob(account));
        }
      }
    },
    methods: {
      login() {
        this.$refs["loginForm"].validate(valid => {
          if (valid) {
            this.$store
              .dispatch("auth/login", {
                ...this.loginForm,
                callback: () => {
                  if (this.remember) {
                    localStorage.setItem("remmemberMe", true);
                    localStorage.setItem(this.accountKey, window.btoa(JSON.stringify(this.loginForm)));
                  } else {
                    localStorage.removeItem("remmemberMe");
                    localStorage.removeItem(this.accountKey);
                  }
                },
              })
              .catch(() => (this.err_msg = this.$t("loginErr")));
            // 这里看是否需要记住密码
          }
        });
      },
    },
  };
</script>

<style lang="scss" scoped>
  .forgot-btn {
    font-size: 12px;
    font-weight: bold;
    color: rbg(37, 61, 172);
    // margin-left: 10px;
    display: block;
    text-decoration: underline;
  }
  .other-btn {
    margin-top: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
</style>
