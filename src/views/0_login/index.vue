<template>
  <div class="login" v-loading="pageLoading">

    <section :class="['content', {'content-reginster': !registered}]">
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

      <div class="login-content" v-if="registered">
        <div class="login-title">
          <span class="dynamic-title" v-if="$INDUSTRY">{{ $t("header.power")}}</span>
          <span class="dynamic-title" v-else>{{ $t("systemTitle") }}</span>
        </div>
        <el-form :model="dynamicValidateForm" ref="dynamicValidateForm" :rules="formRules" label-width="0px"
          class="demo-dynamic">
          <div style="margin-bottom: 20px">
            <p class="lable-form">
              <span>{{ $t("login.username") }}</span>
            </p>
            <el-form-item prop="username" label>
              <el-input ref="username" :placeholder="$t('login.usernamePlaceholder')" v-model="dynamicValidateForm.username"></el-input>
            </el-form-item>
          </div>
          <div>
            <p class="lable-form">
              <span>{{ $t("login.password") }}</span>
            </p>
            <el-form-item label prop="password">
              <el-input v-model="dynamicValidateForm.password" type="password" show-password @keyup.enter.native="submitForm('dynamicValidateForm')" ></el-input>
            </el-form-item>
          </div>

          <el-form-item style="margin-bottom: 30px">
            <el-button type="primary" @click="submitForm('dynamicValidateForm')" class="signin" v-loading="loading">{{
              $t("login.signin") }}</el-button>
          </el-form-item>
        </el-form>
        <div class="language" @click="switchLanguage">{{ locallanguage }}</div>
      </div>
      <div class="login-content reginster-box" v-else>
        <div class="login-title">
          <span class="dynamic-title">{{ $t("register.title") }}</span>
          <span class="activate-tip">{{ $t("register.titleTip") }}</span>
        </div>
        <el-form :model="registerValidateForm" ref="registerValidateForm" :rules="registerFormRules" label-width="0px"
          class="demo-dynamic">
          
          <div style="margin-bottom: 20px">
            <p class="lable-form">
              <span>{{ $t("register.name") }}</span>
            </p>
            <el-form-item prop="name" label>
              <el-input ref="name" :placeholder="$t('register.nameTips')" v-model="registerValidateForm.name"></el-input>
            </el-form-item>
          </div>
          <div style="margin-bottom: 20px">
            <p class="lable-form">
              <span>{{ isLocaleLanguageEn ? $t("register.email") : $t("register.phone") }}</span>
            </p>
            <el-form-item prop="phone_email" label>
              <el-input ref="phone_email" :placeholder="$t('register.phoneTips')" v-model="registerValidateForm.phone_email"></el-input>
            </el-form-item>
          </div>
          <div>
            <p class="lable-form">
              <span>{{ $t("register.verificationCode") }}</span>
            </p>
            <el-form-item label prop="verification_code">
              <el-input v-model="registerValidateForm.verification_code" @keyup.enter.native="submitRegisterForm('registerValidateForm')" >
                <el-button type="primary" slot="append"
                  :disabled="disableGetVerificationCode"
                  style="min-width: 180px;" 
                  @click="handlerCaptcha">
                  {{ buttonTextOfGetVerificationCode }}
                </el-button>
              </el-input>
            </el-form-item>
          </div>

          <el-form-item style="margin-bottom: 30px">
            <el-button type="primary" @click="submitRegisterForm('registerValidateForm')" class="signin" v-loading="loading">{{
              $t("register.signin") }}</el-button>
          </el-form-item>
        </el-form>
        
        <el-alert
          :title="$t('register.requirement')"
          type="warning">
        </el-alert>
        <div class="language" @click="switchLanguage">{{ locallanguage }}</div>
      </div>
      
    </section>

    <div class="copyright" v-if="!oemName">
      <span>{{ $t("copyright") }}</span>
    </div>
    <el-dialog :title="$t('register.imageVerificationCode')" :visible.sync="visible" width="400px" center :close-on-click-modal="false">
      <el-form ref="captchaForm" :model="captchaForm" :rules="captchaRulus" @submit.native.prevent>
        <el-form-item label="">
          <el-input v-model="captchaForm.captchaCode" ref="captcha" class="captcha-input" @keyup.enter.native="handlerVerificationCode" autocomplete="off">
            <div slot="append" class="captcha-img-box">
              <img height="40px" @click="handlerCaptcha" :src="imageUrl" />
            </div>
          </el-input>
        </el-form-item>
      </el-form>
    <div slot="footer" class="dialog-footer" style="text-align: right">
      <el-button type="primary" size="small" @click="handlerVerificationCode">{{ $t('confirm') }}</el-button>
    </div>
  </el-dialog>
  </div>
</template>
<script>
import { DbBase64 } from "../../utils/dbBase64";
import { deleteCookieItem } from "@/utils/index";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import dataJson from "./data.json";
import SearchPop from "@/components/Header/components/pop";
import { getUrls, fetchApiByCluster, fetchIsbinding, fetchVerificationCode, getVerificationResult, fetchCaptcha, reportTaosdInfo } from "@/api/explorer/login";
import { encrypt } from "@/utils/index";
import Vue from 'vue';
import LicenseMixin from "@/mixins/license";

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
        callback();
      }
    };
    var validatePhoneEmail = (rule, value, callback) => {
      if (value === "") {
        if (this.isLocaleLanguageEn) {
          callback(new Error(this.$t("register.emailTips")));
        } else {
          callback(new Error(this.$t("register.phoneTips")));
        }
      } else if (!this.isLocaleLanguageEn) {
        // 校验手机号
        if (!this.checkPhone(value)) {
          callback(new Error(this.$t("register.phoneTips")));
          return;
        }
      } else {
        if (!(this.checkPhone(value) || this.checkEmail(value))) {
          callback(new Error(this.$t("register.emailTips")));
          return;
        }
      }

      callback();
    };
    return {
      taosxStatus: true,
      oemName:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
      loading: false,
      earch: require("@/assets/earth.webp"),
      dynamicValidateForm: {
        cluster: "",
        password: "",
        username: "",
      },
      pageLoading: false,
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
      buttonTextOfGetVerificationCode: this.$t("register.getVerificationCode"),
      registerValidateForm: {
        name: "",
        phone_email: "",
        verification_code: "",
      },
      captchaForm: {
        captchaCode: '',
      },
      registered: true,
      visible: false,
      imageUrl: "",
      disableGetVerificationCode: false,
      captchaRulus: {
        captchaCode: [
          {
            required: true,
            message: this.$t("required")
          }
        ]
      },
      registerFormRules: {
        name: [
          {
            required: true,
            max: 80,
            message: this.$t("register.nameTips"),
            trigger: "change",
          },
        ],
        verification_code: [
          {
            required: true,
            message: this.$t("register.verificationCodeTips"),
            trigger: "change",
          },
        ],
        phone_email: [
          {
            required: true,
            validator: validatePhoneEmail,
            trigger: "change",
          },
        ],
      },
    };
  },
  mixins: [LicenseMixin],
  computed: {
    isLocaleLanguageEn() {
      return this.$i18n.locale.includes('en')
    },
    locallanguage(){
      if(this.$i18n.locale=='zh'){
        return 'EN'
      }else{
        return '中'
      }
    }
  },
  methods: {
    submitForm(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.loading = true;
          this.encryptedPwd = encrypt(this.dynamicValidateForm.password);
          setTimeout(() => {
           
            this.login();
          }, 1000);
        } else {
          return false;
        }
      });
    },
    async getTaosdInfo() {
      try {
        let res=await sendSQLReq(`select id, CONCAT(server_version(), ' ', version) as version from information_schema.ins_cluster`)
        if (res?.code === 0) {
          let id = res.data[0][0].toString();
          localStorage.setItem("local_clusterID", id);
          return [id, res.data[0][1]];
        }
      } catch (error) {
        localStorage.removeItem("TDengine-Token");
        console.log(error);
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

        if (res && res.code == 0 && !res.desc) {
          localStorage.setItem("TDengine-Token", token);
          await this.getUserAuthority();
          await this.getGrantsFull();

          const [cluster_id, taosd_version] = await this.getTaosdInfo();
          const phone_email = sessionStorage.getItem("registerKey");
          const lang = localStorage.getItem('local_language') || '';
          if (phone_email) {
            reportTaosdInfo({
              phone_email,
              lang,
              cluster_id,
              taosd_version,
            }).finally(() => {
              sessionStorage.removeItem("registerKey");
            });
          }

        } else {
          this.loading = false;
          if (res && res.code == 11) {
            this.$error(this.$t("login.servTaosdTip"));
          } else {
            this.$error(res.desc || this.$t("login.errorTip"));
          }
        }
      } catch (error) {
        console.log('error',error);
        this.$error(this.$t("login.servExceptionTip"));
        this.loading = false;
        deleteCookieItem();
      }
    },
    async getClusterAndDashboardUrl() {
      try {
        let res = await getUrls();
        if (res && res.cluster) {
          this.dynamicValidateForm.cluster = res.cluster;
          localStorage.setItem("base_url", this.dynamicValidateForm.cluster);
          this.$store.commit(
            "app/SET_CLUSTER_URL",
            this.dynamicValidateForm.cluster
          );
        }

        if (res.cluster_native) {
          localStorage.setItem("native_url", res.cluster_native);
        } else {
          localStorage.removeItem("native_url");
        }
        
        if (res && res.dashboard) {
          localStorage.setItem("local_grafana", res.dashboard);
        }
        if (res && res.grpc) {
          localStorage.setItem("local_endpoint", res.grpc);
        }
        if (res && res.x_api) {
          this.taosxStatus = true;
        } else {
          this.taosxStatus = false;
        }
      } catch (error) {
        this.$error(error);
      }
    },
    //获取登录用户权限
    async getUserAuthority() {
      try {
        let res=await sendSQLReq(
          `select server_version(), version, (expire_time < now) as valid from information_schema.ins_cluster;`
        )
        if(res?.desc){
          this.$error(res.desc)
          return
        }
        if(res&&res.data){
          let result = res.data.map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            });
            if (
              result.length > 0 &&
              ["official", "trial", "community"].includes(result[0].version)
            ) {
              this.$router.push({
                path: "/explorer",
              });
            } else {
              this.$error(this.$t("login.versiontip"));
            }
        }
        
      } catch (err) {
        this.loading = false;

        if (err && err.code == 11) {
          this.$error(this.$t("login.servTaosdTip"));
          return;
        }
        this.$error(err?.desc);
      }
    },
    async getIsbinding() {
      try {
        const result = await fetchIsbinding();
        if (result && result.code == 0) {
          this.registered = result.data;
        }
        if (this.registered) {
          this.$refs.phone_email.focus();
        }
      } catch (error) {
        console.log('error',error);
      }
    },
    checkPhone(val) {
      return /^1[3456789]\d{9}$/.test(val)
    },
    checkEmail(val) {
      return /^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$/.test(val)
    },

    async handlerCaptcha() {

      if (!this.isLocaleLanguageEn) {
        // 校验手机号
        if (!this.checkPhone(this.registerValidateForm.phone_email)) {
          this.$error(this.$t('register.phoneTips'));
          return;
        }
      } else {
        // 校验邮箱
        if (!this.checkEmail(this.registerValidateForm.phone_email)) {
          this.$error(this.$t('register.emailTips'));
          return;
        }
      }

      // 弹出获取图形验证码的弹框
      this.captchaForm.captchaCode = '';
      this.visible = true;
      this.ts = new Date().getTime()
      const result = await fetchCaptcha(this.registerValidateForm.phone_email, this.ts)
     
      // 有正确的结果才弹框     
      if (result) {
        this.visible = true;
        let imageUrl = URL.createObjectURL(result);
        this.imageUrl = imageUrl;
      }
      this.$nextTick(() => {
        this.$refs.captcha.focus();
      })
    },
    
    async handlerVerificationCode() {
      // 调用获取手机验证码的接口
      // 图形验证码必须填才能调用
      this.$refs.captchaForm.validate(async (valid) => {
        if (!valid) return;
        const result = await fetchVerificationCode(this.registerValidateForm.phone_email,this.captchaForm.captchaCode,this.ts, this.$i18n.locale)
        if (result && result.code == 0) {
          this.$message.success(this.$t('register.success.verificationCodeSend'));
          this.visible = false;

          // 开启验证码倒计时
          let count = 120;
          this.disableGetVerificationCode = true;
          this.timer = setInterval(() => {
            this.buttonTextOfGetVerificationCode = `${count}s`;
            count--;
            if (count <= 0) {
              clearInterval(this.timer);
              this.timer = null;
              this.disableGetVerificationCode = false;
              this.buttonTextOfGetVerificationCode = this.$t('register.regetVerificationCode');
            }
          }, 1000)
        } else if (result) {
          if (result.code == 400) {
            this.$error(this.$t("register.errors." + result.msg));
          } else if (result.code == 501) {
            this.$error(this.$t("register.errors.network"))
          } else {
            this.$error(result.msg)
          }
        }
      })
    },
    submitRegisterForm(formName) {
      this.$refs[formName].validate(async (valid) => {
        if (valid) {
          this.pageLoading = true;
          // 提交注册接口
          this.registerValidateForm.ts = this.ts;
          this.registerValidateForm.lang = localStorage.getItem('local_language') || '';

          const result = await getVerificationResult(this.registerValidateForm)
          if (result && result.code == 0) {
            switch (result.data) {
              case 'pass':
                // 如果校验通过，则注册成功 切换到登陆框
                this.registered = true;
                sessionStorage.setItem('registerKey', this.registerValidateForm.phone_email);

                setTimeout(() => {
                  this.pageLoading = false;
                  this.$message.success(this.$t('register.success.registerSuccess'));
                }, 1000)
                
                break;
              case 'none':
                this.$error(this.$t('register.errors.verificationCodeNone'));
                this.pageLoading = false;
                break;
              case 'error':
                this.$error(this.$t('register.errors.verificationCodeError'));
                this.pageLoading = false;
                break;
            } 
          }
        } else {
          return false;
        }
      });
    },
    switchLanguage() {
      if(this.$i18n.locale=='zh'){
        this.$i18n.locale='en'
        localStorage.setItem("local_language", "en");
      }else{
        this.$i18n.locale='zh'
        localStorage.setItem("local_language", "zh");
      }
    },
  },
  async created() {
    await this.getClusterAndDashboardUrl();
    localStorage.setItem("supportWebsite", this.dataJson.supportWebsite);
    localStorage.setItem("documentWebsite", this.dataJson.documentWebsite);
    if (this.$COMMUNITY) {
      await this.getIsbinding();
    }

  },
  mounted() {
    this.$refs.username.focus();
    this.$nextTick(() => {
      if (
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine"
      ) {
        let dynamic = document.querySelector(".dynamic-title");
        dynamic.innerText = process.env.VUE_APP_CUS_NAME + " Management System";
      }
    })
    const timer = setTimeout(() => {
      Vue.prototype.$message = Message;
    }, 1500)
  },
};
</script>
<style lang="scss" scoped>

.captcha-input {
  ::v-deep .el-input-group__append {
    padding:0;
  }
  .captcha-img-box {
    height:38px;overflow: hidden;
    img {
      margin-top: -1px;
      cursor: pointer;
    }
  }
} 

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
      position: relative;

      .dynamic-title {
        width: 100%;
        overflow: hidden;
        display: block;
        text-overflow: ellipsis;
      }

      .activate-tip {
        color: #909399;
        font-size: 14px !important;
      }

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
    .reginster-box {
      height: 700px;
      width: 680px;
    }
  }
  .content-reginster {
    padding: 60px calc(50vw - 600px);
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

  .language {
    margin-top: 4px;
    margin-right:20px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    border: 1px solid #4d6992;
    border-radius: 50%;
    color: #4d6992;
    display: flex;
    justify-content: center;
    position: absolute;
    top: 20px;
    right: 10px;
  }
}
</style>
