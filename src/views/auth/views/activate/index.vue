<template>
  <div style="text-align: center">
    <template v-if="display">
      <template v-if="isSucc">
        <div class="email-wrapper">
          <el-icon class="el-icon-success email-icon" />
        </div>
        <p>{{ $t("login.activiteSucc") }}</p>
        <div style="margin-top: 20px">
          <el-button style="width: 100%" type="primary" @click="jump">{{ $t("login.jumpNow") }}</el-button>
        </div>
      </template>
      <template v-else>
        <div class="email-wrapper error">
          <el-icon class="el-icon-error email-icon" />
        </div>
        <p>
          {{ errorText ? errorText : $t("login.activiteError") }}
          <el-button :disabled="requestIng" v-if="!errorText" @click="resend" type="text">{{ $t("login.sendAnother") }}</el-button>
        </p>
        <div style="margin-top: 20px">
          <el-button style="width: 100%" type="primary" @click="$router.push('/auth/login')">{{ $t("log-in") }}</el-button>
        </div>
      </template>
    </template>
  </div>
</template>

<script>
import { activite } from "@/api/auth";
import { resendEamil } from "@/api/auth";
export default {
  data() {
    return {
      isSucc: true,
      display: false,
      requestIng: false,
      errorText: "",
      state: "",
    };
  },
  created() {
    this.activite();
  },
  methods: {
    async activite() {
      if (this.$route.query.code) {
        // this.$route.query.code = encodeURIComponent(this.$route.query.code);
        let data = await activite(this.$route.query).catch(err => {
          this.errorText = err.msg;
          return false;
        });
        if (data) {
          let { state, token, tokenType } = data;
          token = tokenType + " " + token;
          this.$store.commit("app/SET_TOKEN", token);
          this.state = state;
          setTimeout(() => this.jump(), 3000);
          this.isSucc = true;
        } else {
          this.isSucc = false;
        }
      } else {
        this.isSucc = false;
        this.errorText = this.$t("emailError");
      }
      this.display = true;
    },
    jump() {
      if (this.state == 1) {
        // 用户状态，如果是1，信息已经完善，跳转到首页
        this.$router.push("/");
      } else if (this.state == 2) {
        // 去完善信息
        this.$router.push("/register");
      }
    },
    async resend() {
      if (this.requestIng) return;
      this.requestIng = true;
      let data = await resendEamil(this.route.query.email).catch(err => {
        this.errorText = err.msg;
        return true;
      });
      if (!data) {
        this.$router.push("/auth/check");
        // 一分钟后才能点击发送
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
.email-wrapper {
  color: #69c384;
  text-align: center;
  font-size: 100px;
  &.error {
    color: $color-danger;
  }
}
.mail-link {
  font-weight: bold;
  display: block;
}
</style>
