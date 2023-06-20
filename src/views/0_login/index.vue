<template>
  <div class="login">
    <section class="header">
      <div class="inside-header">
        <!-- <div class="dynamic-title">TDengine Management System</div> -->
        <div class="site-logo">
          <a
            :href="dataJson.officialWebsite"
            target="_blank"
            title="TD Hero"
            rel="home"
          >
            <img :src="dataJson.logo" alt title="TD Hero" width="200" />
          </a>
        </div>
        <div class="site-navigation">
          <nav class="main-navigation">
            <ul id="menu-menu">
              <li class="gitIframe" v-if="!oemName">
                <a href="https://github.com/taosdata/TDengine">
                  <iframe
                    src="https://tdengine.com/star.html?user=taosdata&amp;repo=TDengine&amp;type=star&amp;count=true"
                    frameborder="0"
                    scrolling="0"
                    width="180"
                    height="32"
                    title="GitHub"
                  ></iframe>
                </a>
              </li>
              <li
                v-for="item in dataJson.externalLinks"
                :key="item.name"
                class="link"
              >
                <a :href="item.url">{{ item.name }}</a>
              </li>

              <!-- <li class="link"><a href="#!" @click="search">Search</a></li>
              <li>
                <a href="javascript:void(0)">
                  <img
                    data-v-28e6c436=""
                    src="https://62edbda222ff1144494a0b29.cdn.rabbitloader.com/62edbda222ff1144494a0b29/rls.s-nw-a28/wp-content/uploads/2022/09/26.03-7-language-menu.png"
                    width="32"
                    height="32"
                    alt="Select language"
                    data-rl-src="https://62edbda222ff1144494a0b29.cdn.rabbitloader.com/62edbda222ff1144494a0b29/rls.s-nw-a28/wp-content/uploads/2022/09/26.03-7-language-menu.png"
                    loading="lazy"
                    class="ls-is-cached rl-lazyloaded"
              />-->
              <!-- <img src="https://62edbda222ff1144494a0b29.cdn.rabbitloader.com/62edbda222ff1144494a0b29/rls.s-nw-a28/wp-content/uploads/2022/09/26.03-7-language-menu.png" alt="" width="100"> -->
              <!-- </a>
              </li>-->
            </ul>
          </nav>
        </div>
      </div>
    </section>
    <section class="content">
      <div class="article">
        <h1 style="font-size: 40px">{{ dataJson.welcome.title }}</h1>
        <h3 style="font-size: 18px">{{ dataJson.welcome.subTitle }}</h3>
        <article>
          <p v-for="(item, index) in dataJson.welcome.mainContent" :key="index">
            <span>{{ index + 1 }}.</span>
            <strong>
              <a :href="item.url" style="text-decoration: underline">
                <span class="anchor">{{ item.achorTitle }}</span>
              </a>
            </strong>
            {{ item.paragraph }}
          </p>
        </article>
      </div>

      <div class="login-content">
        <div class="login-title">
          <span class="dynamic-title">{{ $t("systemTitle") }}</span>
        </div>
        <el-form
          :model="dynamicValidateForm"
          ref="dynamicValidateForm"
          :rules="formRules"
          label-width="0px"
          class="demo-dynamic"
        >
          <div style="margin-bottom: 20px">
            <p class="lable-form">
              <span>{{ $t("login.username") }}</span>
            </p>
            <el-form-item prop="username" label>
              <el-input v-model="dynamicValidateForm.username"></el-input>
            </el-form-item>
          </div>
          <div>
            <p class="lable-form">
              <span>{{ $t("login.password") }}</span>
            </p>
            <el-form-item label prop="password">
              <el-input
                v-model="dynamicValidateForm.password"
                type="password"
              ></el-input>
            </el-form-item>
          </div>

          <el-form-item style="margin-bottom: 30px">
            <el-button
              type="primary"
              @click="submitForm('dynamicValidateForm')"
              class="signin"
              v-loading="loading"
              >{{ $t("login.signin") }}</el-button
            >
          </el-form-item>
        </el-form>
      </div>
    </section>
    <section class="plans"></section>
    <section class="footer">
      <div class="footer-contract">
        <div class="inside">
          <div class="foot-top">
            <div class="left">
              <figure class="logo">
                <a
                  :href="dataJson.officialWebsite"
                  target="_blank"
                  title="TD Hero"
                  rel="home"
                >
                  <img :src="dataJson.logo" alt title="TD Hero" width="100" />
                </a>
              </figure>
              <p class="profile">{{ dataJson.footer.profile }}</p>
            </div>
            <div class="right" v-if="dataJson.footer.contracts">
              <div class="sales">
                <span class="button">Contract us</span>
              </div>
              <div class="social">
                <template v-for="(item, index) in dataJson.footer.contracts">
                  <a :href="item.url" :key="index" class="social-btn">
                    <span :class="item.icon"></span>
                  </a>
                </template>
              </div>
            </div>
          </div>
          <div class="foot-bottom">
            <!-- <div class="copy-right"> -->
            <div class="cp-left">{{ dataJson.footer.copyright }}</div>
            <div class="cp-right">
              <template v-for="(item, index) in dataJson.footer.policies">
                <a :href="item.url" :key="index">{{ item.name }}</a>
              </template>
            </div>
            <!-- </div> -->
          </div>
        </div>
      </div>
    </section>
    <div class="copyright" v-if="!oemName">
      <!-- <span >©  2023</span>
      <span >
        涛思数据 |
        <a
          href="http://www.beian.gov.cn/portal/registerSystemInfo?recordcode=11010502047618"
          target="_blank"
          rel="noopener"
          >京公网安备 11010502047618号</a
        >
        |
        <a href="https://beian.miit.gov.cn/" target="_blank" rel="noopener"
          >京ICP备17069529号-1</a
        >
        | 新版时序数据库 TDengine v3.0</span
      > -->
      <span>{{ $t("copyright") }}</span>
    </div>
    <SearchPop :hidden.sync="hidden"></SearchPop>
  </div>
</template>
<script>
import { DbBase64 } from "../../utils/dbBase64";
import { deleteCookieItem } from "@/utils/index";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import dataJson from "./data.json";
import SearchPop from "@/components/Header/components/pop";
import { getUrls, fetchApiByCluster } from "@/api/explorer/login";
import { encrypt } from "@/utils/index";
export default {
  name: "Login",
  components: {
    SearchPop,
  },
  data() {
    var validatePass = (rule, value, callback) => {
      if (value === "") {
        callback(new Error(this.$t("login.passwordTips")));
      } else {
        // setTimeout(() => {
        //   if (this.dynamicValidateForm.password !== "") {
        //     this.$refs.dynamicValidateForm.validateField("password");
        //   }
        // });

        callback();
      }
    };
    return {
      taosxStatus: true,
      oemName:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
      loading: false,
      earch: require("@/assets/earth.webp"),
      hidden: false,
      dynamicValidateForm: {
        cluster: "",
        password: "",
        username: "",
      },
      formRules: {
        cluster: [
          {
            required: true,
            message: "Please enter the Cluster",
            trigger: "blur",
          },
        ],
        password: [
          {
            required: true,
            validator: validatePass,
            trigger: "blur",
          },
        ],
        username: [
          {
            required: true,
            message: this.$t("login.usernameTips"),
            trigger: "blur",
          },
        ],
      },
      dataJson,
      encryptedPwd: "",
    };
  },
  methods: {
    submitForm(formName) {
      // let reg =
      //   /^(https?:\/\/)?([\da-z.-]+)(\.([a-z.]{2,6}))?(:[\d]{1,5})?([\/\w.-]*)*\/?$/;

      // if (
      //   this.dynamicValidateForm.cluster &&
      //   !reg.test(this.dynamicValidateForm.cluster)
      // ) {
      //   Message.error("Please enter the correct cluster url .");
      //   return;
      // }
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.loading = true;
          this.encryptedPwd = encrypt(this.dynamicValidateForm.password);
          setTimeout(() => {
            // if (!this.taosxStatus) {
            //   Message.error(this.$t("login.taosxtip"));
            //   this.loading = false;
            // } else {
            this.login();
            // }
          }, 1000);
        } else {
          return false;
        }
      });
    },
    resetForm(formName) {
      this.$refs[formName].resetFields();
    },
    search() {
      this.hidden = true;
    },
    async getClusterID() {
      try {
        let res = await sendSQLReq(
          ` select id from information_schema.ins_cluster;`
        );
        if (res.message || res.desc) {
          Message.error(res.message || res.desc);
          return;
        }
        let id = res.data.flat(Infinity).toString();
        localStorage.setItem("local_clusterID", id);
      } catch (err) {
        Message.error(err.message || err.desc);
        localStorage.removeItem("TDengine-Token");
      }
    },
    async login() {
      let token =
        "Basic " +
        DbBase64.encode(
          this.dynamicValidateForm.username +
            ":" +
            this.dynamicValidateForm.password
        );
      this.$store.commit("app/SET_TOKEN", token);
      localStorage.setItem("username", this.dynamicValidateForm.username);
      localStorage.setItem("pwd", this.encryptedPwd);

      this.$store.commit("app/SAVE_LOGIN_INFO", {
        username: this.dynamicValidateForm.username,
        pwd: this.dynamicValidateForm.password,
      });
      try {
        let sql = "select server_version()";
        let res = await fetchApiByCluster(
          this.dynamicValidateForm.cluster,
          token,
          sql
        );
        if (res.message || res.desc) {
          Message.error(res.message || res.desc);
          return;
        }
        if (res && res.code == 0 && !res.desc) {
          localStorage.setItem("TDengine-Token", token);
          this.getClusterID();
          // this.$router.push({
          //   path: "/explorer"
          // });
          this.getUserAuthority();
        } else {
          Message.error(this.$t("login.errorTip"));
        }
        this.loading = false;
      } catch (err) {
        Message.error(err.message || err.desc);
        this.loading = false;
        deleteCookieItem();
      }
    },
    async getClusterAndDashboardUrl() {
      try {
        let res = await getUrls();
        if (res.message || res.desc) {
          Message.error(res.message || res.desc);
          return;
        }
        if (res && res.cluster) {
          this.dynamicValidateForm.cluster = res.cluster;
          localStorage.setItem("base_url", this.dynamicValidateForm.cluster);
          this.$store.commit(
            "app/SET_CLUSTER_URL",
            this.dynamicValidateForm.cluster
          );
        }
        if (res && res.dashboard) {
          localStorage.setItem("local_grafana", res.dashboard);
        }
        if (res && res.x_api) {
          this.taosxStatus = true;
        } else {
          this.taosxStatus = false;
        }
      } catch (err) {
        Message.error(err.message || err.desc);
      }
    },
    //获取登录用户权限
    async getUserAuthority() {
      try {
        let res = await sendSQLReq(
          `select version, (expire_time < now) as valid from information_schema.ins_cluster`
        );
        if (res.message || res.desc) {
          Message.error(res.message || res.desc);
          return;
        }
        if (res) {
          let result = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          if (
            result.length > 0 &&
            ["official", "trial"].includes(result[0].version)
          ) {
            this.$router.push({
              path: "/explorer",
            });
          } else {
            Message.error(this.$t("login.versiontip"));
          }
        }
      } catch (err) {
        Message.error(this.$t("login.versiontip"));
      }
    },
  },
  created() {
    this.getClusterAndDashboardUrl();
    localStorage.setItem("supportWebsite", this.dataJson.supportWebsite);
    localStorage.setItem("documentWebsite", this.dataJson.documentWebsite);
  },
  mounted() {
    this.$nextTick(() => {
      if (
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine"
      ) {
        let dynamic = document.querySelector(".dynamic-title");
        dynamic.innerText = process.env.VUE_APP_CUS_NAME + " Management System";
      }
    });
  },
};
</script>
<style lang="scss" scoped>
.login {
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  height: 100%;
  .lable-form {
    font-size: 16px;
    color: #4d6992;
    font-weight: 600;
    margin-bottom: 10px;
  }
  .header {
    display: none !important;
    width: 100%;
    position: relative;
    height: 123px;
    display: flex;
    justify-content: center;
    align-content: center;
    background-position: 50%;
    background-image: url("https://cloud.tdengine.com/static/img/banner-bg.aedcb8e7.webp");
    background-repeat: no-repeat;
    background-size: cover;
    .dynamic-title {
      font-size: 28px;
      color: #fff;
    }
    .inside-header {
      display: none;
      justify-content: center;
      height: 123px;
      max-width: 1240px;
      padding: 20px;
      // justify-content: space-between;
      align-items: center;
      margin-left: auto;
      margin-right: auto;
      flex: 1;
      .site-logo {
        width: 200px;
        display: none;
      }
      .site-navigation {
        flex: auto;
        display: none;
        justify-content: end;
      }
      .main-navigation {
        display: flex;
        white-space: nowrap;

        #menu-menu {
          display: flex;
          .link a {
            font-size: 17.6px;
            font-weight: 300;
            padding-left: 10px;
            padding-right: 10px;
            letter-spacing: 0;
            color: #fff;
          }
        }
      }
    }
  }
  .content {
    display: flex;
    flex-direction: row;
    padding: 90px calc(50vw - 600px);
    justify-content: center;
    border: none;
    .article {
      padding: 15px;
      flex: 1.5;
      display: none !important;
      // border: 1px solid rgb(54, 42, 185);
      margin-right: 20px;
      display: flex;
      flex-direction: column;
      align-items: center;
      article {
        margin-top: 20px;
      }
      p {
        margin-bottom: 10px;
        word-spacing: 4px;
      }
    }
    .login-content {
      width: 600px;
      height: 500px;
      padding: 70px 55px 55px 55px;
      box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.05);
      .login-title {
        font-size: 28px;
        font-weight: 500;
        text-align: center;
        margin-bottom: 40px;
        span {
          font-size: 28px;
        }
      }
    }
  }
  // .plans {
  //   height: 500px;
  // }
  .footer {
    height: 250px;
    background: rgb(65, 138, 217);
    display: none;
    flex-direction: column;
    .footer-contract {
      display: flex;
      flex-direction: row;
      .inside {
        display: none !important;
        flex-direction: column;
        display: flex;
        padding: 30px 20px 0;
        max-width: 1240px;
        margin-left: auto;
        margin-right: auto;
        z-index: 1;
        position: relative;
        margin-top: 10px;
        .foot-top {
          display: flex;
          .left {
            width: 50%;
            .profile {
              color: #fff;
              margin-top: 10px;
            }
          }
          .right {
            width: 50%;
            display: flex;
            flex-direction: column;
            align-items: flex-end;
            .sales {
              display: flex;
              justify-content: flex-end;
              .button {
                white-space: nowrap;
                background-color: #fff;
                color: #578cf5;
                font-size: 15px;
                font-weight: 600;
                padding: 3px 8px;
                border-radius: 10px;
                margin-bottom: 10px;
                cursor: pointer;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                text-align: center;
                text-decoration: none;
                transition: background-color 0.2s ease-in-out,
                  color 0.2s ease-in-out, border-color 0.2s ease-in-out,
                  opacity 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
              }
            }
            .social {
              text-align: right;
              margin-bottom: 1.5em;
              margin-top: 10px;
              padding: 5px 0;
              line-height: 1.5;
              font-size: 18px;
              display: flex;
              justify-content: flex-end;
              align-items: flex-start;
              clear: both;
              .social-btn {
                display: inline-flex;
                align-items: center;
                text-align: center;
                padding: 6px;
                border-radius: 50px;
                margin-left: 20px;
                border-style: solid;
                border-width: 1px;
                color: #fff;
                justify-content: center;
                transition: background-color 0.2s ease-in-out,
                  color 0.2s ease-in-out, border-color 0.2s ease-in-out,
                  opacity 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
              }
            }
          }
        }
        .foot-bottom {
          display: flex;
          position: relative;
          margin-top: 10px;
          margin-bottom: 2.25em;
          justify-content: space-between;
          box-sizing: 30px;
          box-sizing: border-box;
          &::after {
            content: "";
            position: absolute;
            top: 0px;
            left: 0px;
            right: 0px;
            bottom: 0px;
            height: 2px;
            background: #fff;
          }
          .cp-left {
            color: #fff;
          }
          .cp-right {
            a {
              color: #fff;
              padding-right: 10px;
            }
          }
        }
      }
    }
  }
  .el-button.signin {
    color: #fff;
    background-color: #4259ce;
    border-color: #4259ce;
    width: 100%;
    font-weight: 700;
    padding: 8px 20px;
    font-size: 16px;
    margin-top: 25px;
  }
  .copyright {
    display: flex;
    justify-content: center;
    margin-bottom: 40px;
    span {
      color: #909399;
    }
  }
}
</style>
