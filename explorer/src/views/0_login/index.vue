<template>
  <div v-loading="pageLoading" class="login">
    <section :class="['content', { 'content-registered': !registered }]">
      <div class="article">
        <h1 style="font-size: 40px">{{ dataJson.welcome.title }}</h1>
        <h3 style="font-size: 18px">{{ dataJson.welcome.subTitle }}</h3>
        <article>
          <p v-for="(item, index) in dataJson.welcome.mainContent" :key="index">
            <span>{{ index + 1 }}.</span>
            <strong>
              <a :href="item.url" style="text-decoration: underline">
                <span class="anchor">{{ item.anchorTitle }}</span>
              </a>
            </strong>
            {{ item.paragraph }}
          </p>
        </article>
      </div>

      <div v-if="registered" class="login-content">
        <div class="login-title">
          <span v-if="$INDUSTRY" class="dynamic-title">{{ $t('header.power') }}</span>
          <span v-else class="dynamic-title">{{ displaySystemTitle }}</span>
        </div>
        <el-form
          ref="dynamicValidateFormRef"
          :model="dynamicValidateForm"
          :rules="formRules"
          label-width="0px"
          class="demo-dynamic"
          size="large"
        >
          <div style="margin-bottom: 20px">
            <p class="label-form">
              <span>{{ $t('login.username') }}</span>
            </p>
            <el-form-item prop="username">
              <el-input
                ref="usernameRef"
                v-model="dynamicValidateForm.username"
                :placeholder="$t('login.usernamePlaceholder')"
              ></el-input>
            </el-form-item>
          </div>
          <div>
            <p class="label-form">
              <span>{{ $t('login.password') }}</span>
            </p>
            <el-form-item prop="password">
              <el-input
                v-model="dynamicValidateForm.password"
                type="password"
                show-password
                @keyup.enter="submitForm(dynamicValidateFormRef)"
              ></el-input>
            </el-form-item>
          </div>

          <el-form-item style="margin-bottom: 30px">
            <el-button v-loading="loading" type="primary" class="signin" @click="submitForm(dynamicValidateFormRef)">{{
              $t('login.signin')
            }}</el-button>
          </el-form-item>
        </el-form>
        <div class="language" @click="switchLanguage">{{ locallanguage }}</div>
      </div>
      <div v-else class="login-content register-box">
        <div class="login-title">
          <span class="dynamic-title">{{ $t('register.title') }}</span>
          <span class="activate-tip">{{ $t('register.titleTip') }}</span>
        </div>
        <el-form
          ref="registerValidateFormRef"
          :model="registerValidateForm"
          :rules="registerFormRules"
          label-width="0px"
          class="demo-dynamic"
          size="large"
        >
          <div style="margin-bottom: 20px">
            <p class="label-form">
              <span>{{ $t('register.name') }}</span>
            </p>
            <el-form-item v-if="!isLocaleLanguageEn" prop="username">
              <el-input
                ref="name"
                v-model="registerValidateForm.name"
                :placeholder="$t('register.nameTips')"
              ></el-input>
            </el-form-item>
            <div v-else style="display: flex; justify-content: space-between">
              <el-form-item prop="firstname" style="width: 49%">
                <el-input
                  ref="firstname"
                  v-model="registerValidateForm.firstname"
                  :placeholder="$t('register.firstnameTips')"
                ></el-input>
              </el-form-item>
              <el-form-item prop="lastname" style="width: 49%">
                <el-input
                  ref="lastname"
                  v-model="registerValidateForm.lastname"
                  :placeholder="$t('register.lastnameTips')"
                ></el-input>
              </el-form-item>
            </div>
          </div>
          <div style="margin-bottom: 20px">
            <p class="label-form">
              <span>{{ isLocaleLanguageEn ? $t('register.email') : $t('register.phone') }}</span>
            </p>
            <el-form-item prop="phoneEmailRef" label>
              <el-input
                ref="phone_email"
                v-model="registerValidateForm.phone_email"
                :placeholder="$t('register.phoneTips')"
              ></el-input>
            </el-form-item>
          </div>
          <div>
            <p class="label-form">
              <span>{{ $t('register.verificationCode') }}</span>
            </p>
            <el-form-item label prop="verification_code">
              <el-input
                v-model="registerValidateForm.verification_code"
                @keyup.enter="submitRegisterForm(registerValidateFormRef)"
              >
                <template #append>
                  <el-button
                    type="primary"
                    :disabled="disableGetVerificationCode"
                    style="min-width: 180px"
                    @click="handlerCaptcha"
                  >
                    {{ buttonTextOfGetVerificationCode }}
                  </el-button>
                </template>
              </el-input>
            </el-form-item>
          </div>

          <el-form-item style="margin-bottom: 30px">
            <el-button
              v-loading="loading"
              type="primary"
              class="signin"
              @click="submitRegisterForm(registerValidateFormRef)"
              >{{ $t('register.signin') }}</el-button
            >
          </el-form-item>
        </el-form>

        <el-alert :title="$t('register.requirement')" type="warning"> </el-alert>
        <div class="language" @click="switchLanguage">{{ locallanguage }}</div>
      </div>
    </section>

    <div v-if="!$IS_OEM" class="copyright">
      <span>{{ $t('login.copyright') }}</span>
    </div>
    <el-dialog
      v-model="visible"
      :title="$t('register.imageVerificationCode')"
      width="400px"
      center
      :close-on-click-modal="false"
    >
      <el-form ref="captchaFormRef" :model="captchaForm" :rules="captchaRules" @submit.prevent>
        <el-form-item label="">
          <el-input
            ref="captchaRef"
            v-model="captchaForm.captchaCode"
            class="captcha-input"
            autocomplete="off"
            @keyup.enter="handlerVerificationCode(captchaFormRef)"
          >
            <template #append>
              <div class="captcha-img-box">
                <img height="40px" :src="imageUrl" @click="handlerCaptcha" />
              </div>
            </template>
          </el-input>
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer" style="text-align: right">
          <el-button type="primary" size="default" @click="handlerVerificationCode(captchaFormRef)">{{
            $t('confirm')
          }}</el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import { DbBase64 } from '../../utils/dbBase64';
import { deleteCookieItem, getLocalLang } from '@/utils/index';
import { sendSQLReq } from '@/api/explorer';
import { FormInstance } from 'element-plus';
import dataJson from './data.json';
import {
  getUrls,
  fetchApiByCluster,
  fetchIsbinding,
  fetchVerificationCode,
  getVerificationResult,
  fetchCaptcha,
  reportTaosdInfo,
  firstLoginWith
} from '@/api/login';
import { encrypt } from '@/utils/index';
import useLicense from '@/hooks/useLicense';
import { useRouter } from 'vue-router';
import { useStore } from 'vuex';
import i18n from '@/lang';
import { setLocale } from 'taos-ui/config';
const { t } = useI18n();
const store = useStore();
const router = useRouter();
const { getGrantsFull } = useLicense();
const { $IS_COMMUNITY, $IS_TSDBLITE, $IS_OEM, $INDUSTRY, $error } = inject(
  'globalCustomProperties'
) as GlobalCustomProperties;
const usernameRef = ref<HTMLElement | null>();
const phoneEmailRef = ref<HTMLElement | null>();
const captchaRef = ref<HTMLElement | null>();
const dynamicValidateFormRef = ref<FormInstance>();
const captchaFormRef = ref<FormInstance>();
const registerValidateFormRef = ref<FormInstance>();

const validatePass = (rule: any, value: string, callback: (arg0?: Error | undefined) => void) => {
  if (value === '') {
    callback(new Error(t('login.passwordTips')));
  } else {
    callback();
  }
};
const validatePhoneEmail = (rule: any, value: string, callback: (arg0?: Error | undefined) => void) => {
  if (value === '') {
    if (isLocaleLanguageEn.value) {
      callback(new Error(t('register.emailTips')));
    } else {
      callback(new Error(t('register.phoneTips')));
    }
  } else if (!isLocaleLanguageEn.value) {
    // 校验手机号
    if (!checkPhone(value)) {
      callback(new Error(t('register.phoneTips')));
      return;
    }
  } else {
    if (!(checkPhone(value) || checkEmail(value))) {
      callback(new Error(t('register.emailTips')));
      return;
    }
  }

  callback();
};

const taosxStatus = ref<boolean>(false);
const loading = ref<boolean>(false);
const ts = ref();
const timer = ref();
const dynamicValidateForm = reactive({
  cluster: '',
  password: '',
  username: ''
});
const pageLoading = ref(false);
const formRules = reactive({
  cluster: [
    {
      required: true,
      message: 'Please enter the Cluster',
      trigger: 'blur'
    }
  ],
  password: [
    {
      required: true,
      validator: validatePass,
      trigger: 'blur'
    }
  ],
  username: [
    {
      required: true,
      message: t('login.usernameTips'),
      trigger: 'blur'
    }
  ]
});
// dataJson,
const encryptedPwd = ref('');
const buttonTextOfGetVerificationCode = ref(t('register.getVerificationCode'));
const registerValidateForm = reactive({
  ts: '',
  lang: '',
  name: '',
  firstname: '',
  lastname: '',
  phone_email: '',
  verification_code: ''
});
const captchaForm = reactive({
  captchaCode: ''
});
const registered = ref<boolean>(true); // for test
const registerKey = ref<string>('');
const visible = ref<boolean>(false);
const imageUrl = ref<string>('');
const disableGetVerificationCode = ref(false);
const captchaRules = reactive({
  captchaCode: [
    {
      required: true,
      message: t('required')
    }
  ]
});
const registerFormRules = reactive({
  name: [
    {
      required: true,
      min: 2,
      max: 80,
      message: t('register.nameTips'),
      trigger: 'change'
    }
  ],
  firstname: [
    {
      required: true,
      max: 80,
      message: t('register.firstnameTips'),
      trigger: 'change'
    }
  ],
  lastname: [
    {
      required: true,
      max: 80,
      message: t('register.lastnameTips'),
      trigger: 'change'
    }
  ],
  verification_code: [
    {
      required: true,
      message: t('register.verificationCodeTips'),
      trigger: 'change'
    }
  ],
  phone_email: [
    {
      required: true,
      validator: validatePhoneEmail,
      trigger: 'change'
    }
  ]
});

const isLocaleLanguageEn = computed(() => {
  return getLocalLang().includes('en');
});
const locallanguage = computed(() => {
  if (getLocalLang() == 'zh') {
    return 'EN';
  } else {
    return '中';
  }
});

const displaySystemTitle = ref( import.meta.env.VITE_APP_CUS_NAME + t('login.systemTitle'))

async function init() {
  await getClusterAndDashboardUrl();
  localStorage.setItem('supportWebsite', dataJson.supportWebsite);
  localStorage.setItem('documentWebsite', dataJson.documentWebsite);
  if ($IS_COMMUNITY && !$IS_TSDBLITE) {
    await getIsbinding();
  }
}
init();
onMounted(() => {
  usernameRef.value?.focus();
  nextTick(() => {
    // if (import.meta.env.VITE_APP_CUS_NAME && import.meta.env.VITE_APP_CUS_NAME !== 'TDengine') {
    //   const dynamic: HTMLElement = document.querySelector('.dynamic-title') as HTMLElement;
    //   dynamic.innerText = import.meta.env.VITE_APP_CUS_NAME + ' Management System';
    // }
  });
});

function submitForm(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(valid => {
    if (valid) {
      loading.value = true;
      encryptedPwd.value = encrypt(dynamicValidateForm.password);
      setTimeout(() => {
        login();
      }, 1000);
    } else {
      return false;
    }
  });
}
async function getTaosdInfo() {
  try {
    const res = await sendSQLReq(
      `select id, CONCAT(server_version(), ' ', version) as version from information_schema.ins_cluster`
    );
    if (res?.code === 0) {
      const id = res.data[0][0].toString();
      localStorage.setItem('local_clusterID', id);
      return [id, res.data[0][1]];
    } else {
      console.error('Failed to get taosd info:', res?.desc || 'Unknown error');
      return ['', ''];
    }
  } catch (error: any) {
    if (error.includes && error.includes('Permission denied')) {
      console.log('User login without sysinfo', error);
      console.log(`app: ${store.state.app.sysinfo}`);
      store.state.app.sysinfo = false;
      console.log(`app: ${store.state.app.sysinfo}`);
      return ['', ''];
    }
    localStorage.removeItem('TDengine-Token');
    console.log(error);
    return Promise.reject(error);
  }
}
async function login() {
  const token = 'Basic ' + DbBase64.encode(dynamicValidateForm.username + ':' + dynamicValidateForm.password);
  store.commit('app/SET_TOKEN', token);
  localStorage.setItem('username', dynamicValidateForm.username);
  localStorage.setItem('pwd', encryptedPwd.value);

  store.commit('app/SAVE_LOGIN_INFO', {
    username: dynamicValidateForm.username,
    pwd: dynamicValidateForm.password
  });
  try {
    const sql = 'select server_version()';
    const res = await firstLoginWith(token, sql);

    if (res && res.code == 0 && !res.desc) {
      localStorage.setItem('TDengine-Token', token);
      const server_version = res.data[0][0];
      const registered_user = res.registered_user || '';
      if (registered_user) {
        registerKey.value = registered_user;
        sessionStorage.setItem('registerKey', registered_user);
        registered.value = true;
      }
      await getGrantsFull();
      await getUserAuthority();

      let [cluster_id, taosd_version] = await getTaosdInfo();
      if (!cluster_id) {
        cluster_id = 'unknown';
        localStorage.setItem('local_clusterID', cluster_id);
      }
      if (!taosd_version) {
        taosd_version = server_version;
        localStorage.setItem('td_version', taosd_version);
      }
      const phone_email = registered_user;
      const lang = localStorage.getItem('local_language') || '';
      if (phone_email) {
        reportTaosdInfo({
          phone_email,
          lang,
          cluster_id,
          taosd_version
        });
      }
    } else {
      loading.value = false;
      if (res && res.code == 11) {
        $error(t('login.servTaosdTip'));
      } else {
        $error(res.desc || t('login.errorTip'));
      }
    }
  } catch (error) {
    console.log('error', error);
    $error(t('login.servExceptionTip'));
    loading.value = false;
    deleteCookieItem();
  }
}
async function getClusterAndDashboardUrl() {
  try {
    const res: ProfileResult = await getUrls();
    if (res && res.cluster) {
      dynamicValidateForm.cluster = res.cluster;
      localStorage.setItem('base_url', dynamicValidateForm.cluster);
      store.commit('app/SET_CLUSTER_URL', dynamicValidateForm.cluster);
    }

    if (res && res.cluster_native) {
      localStorage.setItem('native_url', res.cluster_native);
    }

    if (res && res.grafana && res.grafana.dashboards) {
      const grafana_dashboards = [];
      for (const key in res.grafana.dashboards) {
        grafana_dashboards.push({
          key,
          url: res.grafana.dashboards[key].replace(/^https?:\/\/[^/]+/, '')
        });
      }
      if (grafana_dashboards.length > 0) {
        localStorage.setItem('local_grafana', JSON.stringify(grafana_dashboards));
      } else {
        localStorage.removeItem('local_grafana');
      }
    }

    if (res && res.grpc) {
      localStorage.setItem('local_endpoint', res.grpc);
    }
    if (res && res.x_api) {
      taosxStatus.value = true;
    } else {
      taosxStatus.value = false;
    }
  } catch (error) {
    $error(error);
  }
}
//获取登录用户权限
async function getUserAuthority() {
  try {
    const res = await sendSQLReq(
      `select server_version(), version, (expire_time < now) as valid from information_schema.ins_cluster;`
    );
    if (res?.desc) {
      $error(res.desc);
      return;
    }
    if (res && res.data) {
      const result = res.data.map(data => {
        return Object.fromEntries(
          res.column_meta.map((item, index) => {
            return [item[0], data[index]];
          })
        );
      });
      store.state.app.sysinfo = true;
      if (result.length > 0) {
        console.log(result[0].version);
        if (result[0].version === 'official') {
          await router.push({
            path: '/explorer'
          });
        } else {
          const phone_email = registerKey.value || '';
          if (!phone_email) {
            await router.push({
              path: '/register'
            });
          } else {
            await router.push({
              path: '/explorer'
            });
          }
        }
      } else {
        $error(t('login.versiontip'));
      }
    }
  } catch (err: any) {
    loading.value = false;

    if (err && err.includes('Permission denied')) {
      console.log('User login without sysinfo');
      store.state.app.sysinfo = false;
      await router.push({
        path: '/explorer'
      });
      return;
    }
    $error(err?.desc);
  }
}
async function getIsbinding() {
  try {
    const result = await fetchIsbinding();
    if (result && result.code == 0) {
      registered.value = result.data;
    }
    if (registered.value) {
      nextTick(() => {
        phoneEmailRef.value?.focus();
      });
    }
  } catch (error) {
    console.log('error', error);
  }
}
function checkPhone(val) {
  return /^1[3456789]\d{9}$/.test(val);
}
function checkEmail(val) {
  return /^[.a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$/.test(val);
}

async function handlerCaptcha() {
  if (!isLocaleLanguageEn.value) {
    // 校验手机号
    if (!checkPhone(registerValidateForm.phone_email)) {
      $error(t('register.phoneTips'));
      return;
    }
  } else {
    // 校验邮箱
    if (!checkEmail(registerValidateForm.phone_email)) {
      $error(t('register.emailTips'));
      return;
    }
  }

  // 弹出获取图形验证码的弹框
  captchaForm.captchaCode = '';
  visible.value = true;
  ts.value = new Date().getTime();
  const result = await fetchCaptcha(registerValidateForm.phone_email, ts);

  // 有正确的结果才弹框
  if (result) {
    visible.value = true;
    imageUrl.value = URL.createObjectURL(result);
  }
  nextTick(() => {
    captchaRef.value?.focus();
  });
}

async function handlerVerificationCode(formEl: FormInstance | undefined) {
  // 调用获取手机验证码的接口
  // 图形验证码必须填才能调用
  if (!formEl) return;
  formEl.validate(async valid => {
    if (!valid) return;
    const result = await fetchVerificationCode(
      registerValidateForm.phone_email,
      captchaForm.captchaCode,
      ts.value,
      getLocalLang()
    );
    if (result && result.code == 0) {
      ElMessage.success(t('register.success.verificationCodeSend'));
      visible.value = false;

      // 开启验证码倒计时
      let count = 120;
      disableGetVerificationCode.value = true;
      timer.value = setInterval(() => {
        buttonTextOfGetVerificationCode.value = `${count}s`;
        count--;
        if (count <= 0) {
          clearInterval(timer.value);
          timer.value = null;
          disableGetVerificationCode.value = false;
          buttonTextOfGetVerificationCode.value = t('register.regetVerificationCode');
        }
      }, 1000);
    } else if (result) {
      if (result.code == 400) {
        $error(t('register.errors.' + result.msg));
      } else if (result.code == 501) {
        $error(t('register.errors.network'));
      } else {
        $error(result.msg);
      }
    }
  });
}
function submitRegisterForm(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (valid) {
      pageLoading.value = true;

      const formData: any = {
        ts: ts.value,
        lang: getLocalLang(),
        phone_email: registerValidateForm.phone_email,
        verification_code: registerValidateForm.verification_code
      };
      if (!isLocaleLanguageEn.value) {
        formData['name'] = registerValidateForm.name;
      } else {
        formData['firstname'] = registerValidateForm.firstname;
        formData['lastname'] = registerValidateForm.lastname;
      }

      // 提交注册接口
      const result = await getVerificationResult(formData);
      if (result && result.code == 0) {
        switch (result.data) {
          case 'pass':
            // 如果校验通过，则注册成功 切换到登陆框
            registered.value = true;
            sessionStorage.setItem('registerKey', formData.phone_email);

            setTimeout(() => {
              pageLoading.value = false;
              ElMessage.success(t('register.success.registerSuccess'));
            }, 1000);

            break;
          case 'none':
            $error(t('register.errors.verificationCodeNone'));
            pageLoading.value = false;
            break;
          case 'error':
            $error(t('register.errors.verificationCodeError'));
            pageLoading.value = false;
            break;
        }
      }
    } else {
      return false;
    }
  });
}
function switchLanguage() {
  if (getLocalLang() == 'zh') {
    i18n.global.locale.value = 'en';
    localStorage.setItem('local_language', 'en');
    setLocale('en');
  } else {
    i18n.global.locale.value = 'zh';
    localStorage.setItem('local_language', 'zh');
    setLocale('zh');
  }
  buttonTextOfGetVerificationCode.value = t('register.getVerificationCode');

  dynamicValidateFormRef.value?.resetFields();
  registerValidateFormRef.value?.resetFields();
  formRules.username[0].message = t('login.usernameTips');

  displaySystemTitle.value = import.meta.env.VITE_APP_CUS_NAME + t('login.systemTitle');
}
</script>
<style lang="scss" scoped>
.captcha-input {
  :deep(.el-input-group__append) {
    padding: 0;
  }

  .captcha-img-box {
    height: 38px;
    overflow: hidden;

    img {
      margin-top: -1px;
      cursor: pointer;
    }
  }
}

.login {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow-y: auto;

  .label-form {
    margin-bottom: 10px;
    font-size: 16px;
    font-weight: 600;
    color: #4d6992;
  }

  .header {
    position: relative;
    display: none !important;
    display: flex;
    place-content: center center;
    width: 100%;
    height: 123px;
    background-image: url('https://cloud.tdengine.com/static/img/banner-bg.aedcb8e7.webp');
    background-repeat: no-repeat;
    background-position: 50%;
    background-size: cover;

    .dynamic-title {
      font-size: 28px;
      color: #fff;
    }

    .inside-header {
      display: none;
      flex: 1;

      // justify-content: space-between;
      align-items: center;
      justify-content: center;
      max-width: 1240px;
      height: 123px;
      padding: 20px;
      margin-right: auto;
      margin-left: auto;

      .site-logo {
        display: none;
        width: 200px;
      }

      .site-navigation {
        display: none;
        flex: auto;
        justify-content: end;
      }

      .main-navigation {
        display: flex;
        white-space: nowrap;

        #menu-menu {
          display: flex;

          .link a {
            padding-right: 10px;
            padding-left: 10px;
            font-size: 17.6px;
            font-weight: 300;
            color: #fff;
            letter-spacing: 0;
          }
        }
      }
    }
  }

  .content {
    display: flex;
    flex-direction: row;
    justify-content: center;
    padding: 90px calc(50vw - 600px);
    border: none;

    .article {
      display: none !important;
      display: flex;
      flex: 1.5;
      flex-direction: column;
      align-items: center;
      padding: 15px;

      // border: 1px solid rgb(54, 42, 185);
      margin-right: 20px;

      article {
        margin-top: 20px;
      }

      p {
        margin-bottom: 10px;
        word-spacing: 4px;
      }
    }

    .login-content {
      position: relative;
      width: 600px;
      height: 500px;
      padding: 70px 55px 55px;
      box-shadow: 0 2px 12px 0 rgb(0 0 0 / 5%);

      .dynamic-title {
        display: block;
        width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .activate-tip {
        font-size: 14px !important;
        color: #909399;
      }

      .login-title {
        margin-bottom: 40px;
        font-size: 28px;
        font-weight: 500;
        text-align: center;

        span {
          font-size: 28px;
        }
      }
    }

    .register-box {
      width: 680px;
      height: 700px;
    }
  }

  .content-registered {
    padding: 60px calc(50vw - 600px);
  }

  .el-button.signin {
    width: 100%;
    padding: 8px 20px;
    margin-top: 25px;
    font-size: 16px;
    font-weight: 700;
    color: #fff;
    background-color: #4259ce;
    border-color: #4259ce;
  }

  .copyright {
    display: flex;
    justify-content: center;
    margin-bottom: 40px;
    color: #909399;
  }

  .language {
    position: absolute;
    top: 20px;
    right: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    margin-top: 4px;
    margin-right: 20px;
    color: #4d6992;
    cursor: pointer;
    border: 1px solid #4d6992;
    border-radius: 50%;
  }
}
</style>
