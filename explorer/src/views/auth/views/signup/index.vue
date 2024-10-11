<template>
  <div>
    <el-form :hide-required-asterisk="true" :model="registerForm" :rules="rules" :status-icon="true" ref="registerForm" style="margin-top: 20px">
      <el-form-item :label="$t('fullName')" prop="username">
        <el-input v-model.trim="registerForm.username" />
      </el-form-item>
      <el-form-item :label="$t('email')" prop="email">
        <el-input v-model.trim="registerForm.email" @keyup.enter.native="register" :placeholder="$t('email')"></el-input>
      </el-form-item>
      <el-form-item>
        <el-checkbox v-model="agree">{{ $t("login.updateTip") }}</el-checkbox>
      </el-form-item>
      <p class="errorText">{{ err_msg }}</p>
      <el-form-item>
        <el-button
          type="primary"
          class="loginBtn"
          :disabled="!registerForm.email || !registerForm.username"
          @keyup.enter.native="register"
          @click="register"
          >{{ $t("login.createAccBtn") }}</el-button
        >
      </el-form-item>
      <div class="signUpTip" v-html="$t('login.signUpTip')"></div>
    </el-form>
  </div>
</template>

<script>
import { validEmail } from "@/utils/validate.js";
export default {
  data() {
    var checkEmail = async (_, value, callback) => {
      if (!value || !validEmail(value)) {
        return callback(new Error(this.$t("emailError")));
      }
    };

    return {
      registerForm: {
        email: process.env.VUE_APP_EMAIL,
        username: "",
      },
      agree: true,
      rules: {
        email: [{ validator: checkEmail, trigger: "blur" }],
        username: [
          {
            required: true,
            message: this.$t("register.nameError"),
            trigger: "blur",
          },
          {
            min: 4,
            max: 32,
            message: this.$t("register.nameError"),
            trigger: "blur",
          },
        ],
      },
      err_msg: "",
    };
  },
  methods: {
    register() {
      this.$refs["registerForm"].validate(valid => {
        if (valid) {
          // TODO 添加重定向
          this.$store.dispatch("auth/register", this.registerForm).catch(err_msg => {
            if (err_msg == "1") {
              this.$router.push({
                path: "/auth/registered/" + this.registerForm.email + "/success",
              });
            } else if (err_msg == "2" || err_msg == "0") {
              this.$router.push({
                path: "/auth/registered/" + this.registerForm.email,
              });
            } else {
              this.err_msg = err_msg;
            }
          });
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.errorText {
  color: #ff4949;
  font-size: 12px;
  padding: 0px;
  margin: 0;
  position: absolute;
}

.loginBtn {
  width: 100%;
  // margin-top: 20px;
  text-align: center;
}
.signUpTip {
  margin-top: 20px;
  text-align: center;
  line-height: 1.4;
}
</style>
<style lang="scss">
.password-tip {
  ul {
    list-style: decimal !important;
  }
}
.signUpTip .link {
  text-decoration: underline;
}
</style>
