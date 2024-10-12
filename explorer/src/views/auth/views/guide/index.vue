<template>
  <div class="guide">
    <p class="title">
      {{ $t("login.thanks") }}
    </p>
    <p class="check-content" v-html="thankContent"></p>
    <div class="back-btn">
      <el-button :disabled="requestIng" @click="goBack" type="primary">{{
        $t("login.backLogin")
      }}</el-button>
    </div>
  </div>
</template>

<script>
import { resendEamil } from "@/api/auth";
export default {
  props: {
    email: {
      type: String,
      default: ""
    }
  },
  data() {
    return {
      requestIng: false
    };
  },
  computed: {
    thankContent() {
      return this.$t("login.thankTip").replace(/\{email\}/g, this.email);
    }
  },
  methods: {
    goBack() {
      this.$router.push("/auth/login");
    },
    async resend() {
      if (this.requestIng) return;
      this.requestIng = true;
      let data = await resendEamil(this.email).catch(() => true);
      if (!data) {
        this.$message.success(this.$t("sendSucc"));
        // 一分钟后才能点击发送
        setTimeout(() => {
          this.requestIng = false;
        }, 60 * 1000);
      } else {
        this.requestIng = false;
      }
    }
  }
};
</script>

<style lang="scss" scoped>
.guide {
  text-align: center;
  margin-top: 20px;
  font-size: 16px;
}
.title {
  font-size: 24px;
  font-weight: bold;
  margin-bottom: 20px;
}
.check-content {
  color: #3f4b5f;
}
.back-btn {
  margin-top: 20px;
  &::v-deep .el-button {
    width: 100%;
  }
}
</style>
<style lang="scss">
.mail-link {
  color: $color-primary !important;
  text-decoration: underline;
}
</style>
