<template>
  <div>
    <el-form :hide-required-asterisk="true" :model="forgotForm" :rules="rules" :status-icon="true" ref="forgotForm" style="margin-top: 20px">
      <!-- <el-form-item label="Usernmae" prop="Usernmae">
        <el-input v-model.trim="forgotForm.Usernmae"></el-input>
      </el-form-item> -->
      <el-form-item v-if="!isSend" :label="$t('email')" prop="email">
        <el-input v-model.trim="forgotForm.email" @keyup.enter.native="send" :placeholder="$t('email')"></el-input>
      </el-form-item>
      <el-form-item v-else>
        <p class="tip-icon"><el-icon class="el-icon-success" /></p>
        <p class="tip-text" v-html="sendTip"></p>
      </el-form-item>
      <!-- <el-form-item :label="$t('password')" prop="password">
        <el-popover title="提示" trigger="hover" :content="$t('passwordTip')">
          <el-input
            slot="reference"
            v-model.trim="forgotForm.password"
            maxlength="16"
            minlength="8"
            
          ></el-input>
        </el-popover>
      </el-form-item>
      <el-form-item :label="$t('confirmPass')" prop="confirm">
        <el-input
          v-model.trim="forgotForm.confirm"
          maxlength="16"
          minlength="8"
          
        ></el-input>
      </el-form-item>
       -->
      <p class="errorText">{{ errorText }}</p>
      <el-form-item>
        <section class="login-block">
          <el-button type="primary" :disabled="requestIng" class="loginBtn" @click="send">{{ $t("login.resetMyPass") }}</el-button>
        </section>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
  import { validEmail } from "@/utils/validate.js";
  import { sendEmail, resendEamil } from "@/api/auth";
  export default {
    data() {
      var checkEmail = async (_, value, callback) => {
        this.errorText = "";
        if (!value || !validEmail(value)) {
          return callback(new Error(this.$t("emailError")));
        }
      };

      return {
        forgotForm: {
          email: process.env.VUE_APP_EMAIL,
        },
        isSend: false,
        rules: {
          email: [{ validator: checkEmail, trigger: "blur" }],
        },
        requestIng: false,
        errorText: "",
      };
    },
    computed: {
      sendTip() {
        return this.$t("login.sendedTip").replace(/emailCode/, `<span class='email'>${this.forgotForm.email}</span>`);
      },
    },
    created() {
      if (this.$route.params.email) {
        this.forgotForm.email = this.$route.params.email;
      }
    },
    methods: {
      send() {
        if (this.requestIng) return;
        // if (this.isSend) return this.resendEmail();
        this.$refs["forgotForm"].validate(async valid => {
          if (valid) {
            this.requestIng = true;
            let status = await sendEmail(this.forgotForm.email).catch(err => (this.errorText = err.msg));
            if (!status) {
              this.isSend = true;
              this.$router.push("/auth/check");
              setTimeout(() => {
                this.requestIng = false;
              }, 10 * 1000);
            } else {
              this.requestIng = false;
            }
          }
        });
      },
      // 重新发送发送
      async resendEmail() {
        this.requestIng = true;
        let status = await resendEamil(this.forgotForm.email).catch(() => true);
        if (!status) {
          this.$message.success(this.$t("sendSucc"));
          this.isSend = true;
          setTimeout(() => {
            this.requestIng = false;
          }, 60 * 1000);
        } else {
          this.requestIng = false;
        }
      },
    },
  };
</script>

<style lang="scss" scoped>
  .email {
    font-weight: bold;
    color: #000;
  }
  .tip-icon {
    text-align: center;
    font-size: 50px;
    color: #69c384;
  }
  .tip-text {
    font-size: 16px;
    color: #50576b;
    text-align: center;
  }
</style>
