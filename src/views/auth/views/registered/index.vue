<template>
  <div class="registered">
    <el-form>
      <el-form-item :label="$t('email')" prop="email">
        <el-input
          v-model.trim="email"
          @keyup.enter.native="handle"
          :placeholder="$t('email')"
        ></el-input>
      </el-form-item>
      <p v-show="errorText" class="errorText">{{ errorText }}</p>
      <el-form-item>
        <section class="login-block">
          <el-button
            type="primary"
            class="loginBtn"
            @keyup.enter.native="handle"
            @click="handle"
            >{{
              $t(activated ? "login.resetMyPass" : "login.sendAnother")
            }}</el-button
          >
        </section>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import { sendEmail, resendEamil } from "@/api/auth";
export default {
  props: {
    email: {
      type: String,
      default: ""
    },
    activated: {
      type: String,
      default: ""
    }
  },
  data() {
    return {
      errorText: "",
      requestIng: false
    };
  },
  methods: {
    handle() {
      if (this.requestIng) return;
      this.requestIng = true;
      if (this.activated) {
        sendEmail(this.email)
          .then(() => {
            this.$router.push("/auth/check");
          })
          .catch(err => (this.errorText = err.msg))
          .finally(() => {
            this.requestIng = false;
          });
      } else {
        resendEamil(this.email)
          .then(() => {
            this.$router.push("/auth/login");
          })
          .catch(err => (this.errorText = err.msg))
          .finally(() => {
            this.requestIng = false;
          });
      }
    }
  }
};
</script>

<style lang="scss" scoped>
.registered {
  text-align: center;
  margin-top: 20px;
}
.tip {
  font-size: 14px;
}
</style>
