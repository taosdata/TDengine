<template>
  <div class="auth_layout ">
    <AuthHeader />
    <div class="auth_content">
      <section class="router-wrapper">
        <section class="left-message">
          <h2 class="headline">
            TDengine 
          </h2>
          <ol>
            <li v-for="item in leftMessage.list" :key="item">
              {{ item }}
            </li>
          </ol>
          <h1>{{ leftMessage.title }}!🔥</h1>
        </section>
        <section class="right">
          <el-card class="box-card card">
            <h2 class="title">{{ title }}</h2>
            <div class="subtitle" v-html="subtitle"></div>
            <router-view></router-view>
          </el-card>
          <p v-if="routeName == 'login'" class="sign-up">
            {{ $t("login.noAccount") }}
            <router-link to="/auth/signup"
              >{{ $t("login.createAcc") }}.</router-link
            >
          </p>
          <p v-if="routeName == 'signup'" class="sign-up">
            {{ $t("login.haveAccount") }}
            <router-link to="/auth/login">{{ $t("log-in") }}</router-link>
          </p>
          <p v-if="routeName == 'forgot' || registered" class="sign-up">
            {{ $t("login.rememberedYouPass") }}
            <router-link to="/auth/login">{{ $t("login.login") }}</router-link>
          </p>
          <p v-if="routeName == 'registered' && !registered" class="sign-up">
            {{ $t("login.goBack") }}?
            <router-link to="/auth/login">{{ $t("login.login") }}</router-link>
          </p>
        </section>
      </section>
    </div>
    <AuthFooter />
  </div>
</template>

<script>
import AuthHeader from "@/components/Header";
import AuthFooter from "@/components/Footer";
export default {
  components: { AuthHeader, AuthFooter },
  data() {
    return {};
  },
  computed: {
    leftMessage() {
      return this.$t("login.loginLeftMessage");
    },
    routeName() {
      return this.$route.name;
    },
    title() {
      let result = "";
      switch (this.routeName) {
        case "login":
          result = this.$t("log-in");
          break;
        case "signup":
          result = this.$t("login.signUp");
          break;
        case "forgot":
          result = this.$t("login.forgotpass");
          break;
        case "change":
          result = this.$t("changePass");
          break;
        case "guide":
          result = "";
          break;
        case "check":
          result = this.$t("login.checkEmail");
          break;
        case "activate":
          result = this.$t("login.activate");
          break;
        case "reset":
          result = this.$t("login.resetPass");
          break;
        case "registered":
          result = this.$t("login.youRegistered");
          break;
        default:
          result = this.$t("log-in");
      }
      return result;
    },
    subtitle() {
      let result = "";
      switch (this.routeName) {
        case "login":
          result = "";
          break;
        case "signup":
          result = this.$t("login.freeStart");
          break;
        case "forgot":
          result = this.$t("login.forgotSub");
          break;
        case "check":
          result = this.$t("login.checkTip");
          break;
        case "registered":
          result = this.$t(
            this.registered ? "login.registerForgot" : "login.registerVer"
          );
          break;
      }
      return result;
    },
    registered() {
      return this.routeName == "registered" && this.$route.params.activated;
    }
  }
};
</script>

<style lang="scss" scoped>
$logoSize: 50px;
.auth_layout {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-width: 1200px;
}
.auth_content {
  padding: 45px 0;
  flex: 1;
  @extend .flexCenter;
}
.logoIcon {
  width: $logoSize;
  height: $logoSize;
}

.router-wrapper {
  margin: auto;
  width: 1200px;
  display: flex;
  justify-content: space-around;
  align-items: center;
}

.left-message {
  width: 540px;
  color: #3f4b5f;
  padding-right: 40px;
  line-height: 1.6;
  font-size: 18px;
  font-size: "Graphik Web", Helvetica, Arial, sans-serif;
  ol {
    list-style: decimal outside none;
    margin: 20px 0 20px 20px;
  }
  li {
    list-style: unset;
    line-height: 30px;
    margin-bottom: 10px;
  }
  h1 {
    font-size: 18px;
    word-break: break-word;
    font-weight: bold;
    margin-top: 20px;
  }
}
.right {
  width: 480px;
}
.card {
  padding: 15px 30px;
}
.title {
  font-size: 28px;
  font-weight: 400;
  text-align: center;
}

.subtitle {
  font-size: 16px;
  text-align: center;
  // color: $color-primary;
  margin-top: 5px;
}
.headline {
  width: 100%;
  // margin-top: 40px;
  // text-align: center;
  font-size: 0.8 * $logoSize;
  display: flex;
  align-items: center;
  img {
    margin-right: 10px;
  }
}
.sign-up {
  text-align: center;
  margin-top: 20px;
  font-size: 14px;
  a {
    font-weight: bolder;
    color: $color-primary;
    text-decoration: underline;
  }
}
// .auth_layout ::v-deep .el-button {
//   font-weight: bold;
// }
</style>
<style lang="scss">
.login-block {
  display: flex;
  justify-content: space-between;
  margin-top: 10px;
  align-items: center;
}
.loginBtn {
  width: 100%;
  font-weight: bold;
  padding: 8px 20px;
  font-size: 16px;
}
</style>
