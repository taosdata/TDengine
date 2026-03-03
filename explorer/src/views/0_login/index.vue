<template>
  <div v-loading="pageLoading" class="login">
    <section class="content">
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

      <div class="login-content">
        <div class="login-title">
          <span v-if="$INDUSTRY" class="dynamic-title">{{ $t('header.power') }}</span>
          <span v-else class="dynamic-title">{{ displaySystemTitle }}</span>
        </div>

        <!-- OAuth SSO Login Button -->
        <el-form-item v-if="oauthEnabled && !oauthBind" style="margin-bottom: 20px">
          <el-button class="oauth-button" type="success" @click="loginWithOAuth">
            {{
              $t('login.loginWith', {
                provider: getLocalLang() === 'zh' ? oauthProviderDisplayName.zh : oauthProviderDisplayName.en
              })
            }}
          </el-button>
        </el-form-item>
        <el-divider v-if="oauthEnabled && !oauthBind"> <b>OR</b> </el-divider>
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
                @blur="handleUsernameBlur"
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
    </section>

    <el-dialog
      v-model="captchaVisible"
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
            @keyup.enter="confirmCaptcha(captchaFormRef)"
          >
            <template #append>
              <div class="captcha-img-box">
                <img height="40px" :src="captchaImageUrl" @click="openCaptchaDialog" />
              </div>
            </template>
          </el-input>
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer" style="text-align: right">
          <el-button type="primary" size="default" @click="confirmCaptcha(captchaFormRef)">{{ $t('confirm') }}</el-button>
        </div>
      </template>
    </el-dialog>

    <div v-if="!$IS_OEM" class="copyright">
      <span>{{ $t('login.copyright') }}</span>
    </div>
  </div>
</template>
<script setup lang="ts">
import { DbBase64 } from '../../utils/dbBase64';
import { getLocalLang } from '@/utils/index';
import { sendSQLReq } from '@/api/explorer';
import { FormInstance } from 'element-plus';
import dataJson from './data.json';
import { getUrls, reportTaosdInfo, firstLoginWith, getLoginOptions, fetchCaptcha } from '@/api/login';
import { getOAuthStatus, oauthAuthorize, oauthBindTsdb, oauthMe } from '@/api/oauth';
import { encrypt } from '@/utils/index';
import useLicense from '@/hooks/useLicense';
import { useRouter, useRoute } from 'vue-router';
import { useStore } from 'vuex';
import i18n from '@/lang';
import { setLocale } from 'taos-ui/config';
import Cookies from 'js-cookie';

const { t } = useI18n();
const store = useStore();
const router = useRouter();
const route = useRoute();
const { getGrantsFull } = useLicense();
const { $IS_OEM, $INDUSTRY, $error, OEM_NAME } = inject('globalCustomProperties') as GlobalCustomProperties;
const usernameRef = ref<HTMLElement | null>();
const dynamicValidateFormRef = ref<FormInstance>();

const validatePass = (_rule: any, value: string, callback: (arg0?: Error | undefined) => void) => {
  if (value === '') {
    callback(new Error(t('login.passwordTips')));
  } else {
    callback();
  }
};

const taosxStatus = ref<boolean>(false);
const loading = ref<boolean>(false);
const oauthEnabled = ref<boolean>(false);
const oauthProviderDisplayName = ref<{ en: string; zh: string }>({ en: 'OAuth', zh: 'OAuth' });
const oauthBind = ref<boolean>(false);

const loginCaptchaEnabled = ref<boolean>(false);
// Transient holder for a token (if IdP returns one in the URL). DO NOT persist this
// value to localStorage — server-side httpOnly session cookies are the source of truth.
const oauthTokenFromUrl = ref<string | undefined>(undefined);
const error = ref(false);
const errorMessage = ref('');
const dynamicValidateForm = reactive({
  cluster: '',
  password: '',
  username: '',
  captcha: ''
});

const captchaVisible = ref<boolean>(false);
const captchaImageUrl = ref<string>('');
const captchaTs = ref<number>();
const captchaRef = ref<HTMLElement | null>();
const captchaFormRef = ref<FormInstance>();
const captchaForm = reactive({
  captchaCode: ''
});
const captchaRules = reactive({
  captchaCode: [
    {
      required: true,
      message: t('login.captchaTips'),
      trigger: 'blur'
    }
  ]
});
const trimmedUsername = computed(() => {
  return dynamicValidateForm.username.trim();
});
const trimmedPassword = computed(() => {
  return dynamicValidateForm.password.trim();
});
const pageLoading = ref(true);
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
const registerKey = ref<string>('');

const locallanguage = computed(() => {
  if (getLocalLang() == 'zh') {
    return 'EN';
  } else {
    return '中';
  }
});

const displaySystemTitle = computed(() => OEM_NAME + ' ' + t('login.systemTitle'));

async function init() {
  await getClusterAndDashboardUrl();
  localStorage.setItem('supportWebsite', dataJson.supportWebsite);
  localStorage.setItem('documentWebsite', dataJson.documentWebsite);
}
init();
onMounted(async () => {
  usernameRef.value?.focus();

  // Check whether login CAPTCHA is enabled
  try {
    const opt: any = await getLoginOptions();
    if (opt && opt.code === 0 && opt.data && opt.data.captchaEnabled === true) {
      loginCaptchaEnabled.value = true;
    }
  } catch (e) {
    // ignore and default to disabled
  }

  // Check OAuth status
  try {
    // Do NOT store OAuth tokens in localStorage when using server-side sessions.
    // Verify the server-side session (httpOnly cookie) by calling the profile endpoint.
    try {
      const profileResp = await oauthMe();
      console.log('oauth user', profileResp);
      if (profileResp.tsdb_username) {
        await store.dispatch('app/setOAuthLogin', true);
        ElMessage.success(t('login.oauthLoginSuccess'));
        return await router.push({
          path: '/explorer'
        });
      }
      pageLoading.value = false;
      if (profileResp.user_id) {
        // Session is valid — mark as OAuth login and allow binding flow.
        oauthBind.value = true;
        await store.dispatch('app/setOAuthLogin', true);
        ElMessage.success(t('login.oauthBindSuccess'));
      }
    } catch (e) {
      console.warn('Failed to verify OAuth session:', e);
    }
    pageLoading.value = false;
    const status = await getOAuthStatus();
    oauthEnabled.value = status.enabled;
    if (status.enabled && status.provider_display_name) {
      oauthProviderDisplayName.value = status.provider_display_name;
    }
  } catch (error) {
    console.warn('Failed to get OAuth status:', error);
    pageLoading.value = false;
  }
  nextTick(() => {
    // if (import.meta.env.VITE_APP_CUS_NAME && import.meta.env.VITE_APP_CUS_NAME !== 'TDengine') {
    //   const dynamic: HTMLElement = document.querySelector('.dynamic-title') as HTMLElement;
    //   dynamic.innerText = import.meta.env.VITE_APP_CUS_NAME + ' Management System';
    // }
  });
});

function getOAuthTokenFromURL() {
  // Extract token from URL query parameter
  const token = route.query.token as string;
  const errorParam = route.query.error as string;

  console.log('token: ', token);
  if (errorParam) {
    // OAuth error from backend
    error.value = true;
    errorMessage.value = decodeURIComponent(errorParam);
    loading.value = false;
    ElMessage.error(errorMessage.value);
    return;
  }

  if (!token) {
    error.value = true;
    errorMessage.value = 'No OAuth token received';
    loading.value = false;
    return;
  }
  return token;
}
async function openCaptchaDialog() {
  if (!loginCaptchaEnabled.value) return;

  const username = trimmedUsername.value;
  if (!username) {
    $error(t('login.usernameTips'));
    return;
  }

  captchaForm.captchaCode = '';
  captchaVisible.value = true;

  captchaTs.value = new Date().getTime();
  const result = await fetchCaptcha(username, captchaTs.value);
  if (result) {
    // Release old object URL to avoid leaking memory
    if (captchaImageUrl.value && captchaImageUrl.value.startsWith('blob:')) {
      URL.revokeObjectURL(captchaImageUrl.value);
    }
    captchaImageUrl.value = URL.createObjectURL(result);
  }

  nextTick(() => {
    captchaRef.value?.focus();
  });
}

async function confirmCaptcha(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (!valid) return;

    dynamicValidateForm.captcha = captchaForm.captchaCode;
    captchaVisible.value = false;

    loading.value = true;
    encryptedPwd.value = encrypt(trimmedPassword.value);

    setTimeout(() => {
      login();
    }, 1000);
  });
}

function handleUsernameBlur() {
  if (!loginCaptchaEnabled.value) return;
  dynamicValidateForm.captcha = '';
  if (captchaVisible.value) {
    captchaVisible.value = false;
  }
}

function submitForm(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (valid) {
      if (loginCaptchaEnabled.value) {
        await openCaptchaDialog();
        return;
      }

      loading.value = true;
      encryptedPwd.value = encrypt(trimmedPassword.value);
      setTimeout(() => {
        login();
      }, 1000);
    } else {
      return;
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
async function oauthBindSubmit() {
  loading.value = true;
  try {
    // For server-side session-based OAuth, prefer using the httpOnly session cookie
    // rather than a token persisted in localStorage. If the IdP returned a token in
    // the URL (legacy flows), we'll try to use it, but do NOT persist it client-side.
    const urlToken = (route && route.query && (route.query.token as string)) || undefined;
    // Pass the token if present; otherwise rely on the server to derive session from cookies.
    // Backend should accept session cookie when token is omitted.
    const res = await oauthBindTsdb(trimmedUsername.value, trimmedPassword.value);
    if (res && res.code === 0) {
      ElMessage.success('OAuth account binding successful');
      const sql = 'select server_version()';
      const captcha = loginCaptchaEnabled.value ? (dynamicValidateForm.captcha || '').trim() : undefined;
      const res: any = await firstLoginWith(trimmedUsername.value, trimmedPassword.value, sql, captcha);

      if (res && res.code == 0 && !res.desc) {
        const server_version = res.data[0][0];
        const registered_user = res.registered_user || '';
        if (registered_user) {
          registerKey.value = registered_user;
          sessionStorage.setItem('registerKey', registered_user);
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
        if (phone_email && phone_email != 'skipped') {
          reportTaosdInfo({
            phone_email,
            lang,
            cluster_id,
            taosd_version
          });
        }
      }
      router.push({ path: '/explorer' });
    } else {
      loading.value = false;
      oauthBind.value = false;
      $error(res.desc || 'OAuth account binding failed');
    }
  } catch (error) {
    loading.value = false;
    oauthBind.value = false;
    console.log('OAuth bind error:', error);
    $error(`${t('login.oauthBindError')}: ${error.message}`);
  }
}
async function basicAuthLogin() {
  // Use session-based authentication instead of cookie-based token
  try {
    const sql = 'select server_version()';
    const captcha = loginCaptchaEnabled.value ? (dynamicValidateForm.captcha || '').trim() : undefined;
    const res: any = await firstLoginWith(trimmedUsername.value, trimmedPassword.value, sql, captcha);

    if (res && res.code == 0 && !res.desc) {
      // Store token in memory for the initial request only
      store.commit('app/SET_LOGIN_WITH_SESSION', true);
      localStorage.setItem('username', trimmedUsername.value);
      localStorage.setItem('pwd', encryptedPwd.value);

      store.commit('app/SAVE_LOGIN_INFO', {
        username: trimmedUsername.value,
        pwd: trimmedPassword.value
      });
      const server_version = res.data[0][0];
      const registered_user = res.registered_user || '';
      if (registered_user) {
        registerKey.value = registered_user;
        sessionStorage.setItem('registerKey', registered_user);
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
      if (phone_email && phone_email != 'skipped') {
        reportTaosdInfo({
          phone_email,
          lang,
          cluster_id,
          taosd_version
        });
      }
    } else {
      loading.value = false;
      if (res && (res.desc === 'captchaRequired' || res.desc === 'captchaInputError')) {
        $error(t(`login.${res.desc}`));
        dynamicValidateForm.captcha = '';
        captchaForm.captchaCode = '';
        await openCaptchaDialog();
        return;
      }
      if (res && res.code == 11) {
        $error(t('login.servTaosdTip'));
      } else {
        $error(res.desc || t('login.errorTip'));
      }
    }
  } catch (error) {
    console.log('error', error);
    if (error.response && error.response.data.desc) {
      console.log('api response:', error.response);
      $error(error.response.data.desc);
    } else {
      $error(t('login.servExceptionTip'));
    }
    loading.value = false;
  }
}
async function login() {
  if (oauthBind.value) {
    await oauthBindSubmit();
    return;
  }
  await basicAuthLogin();
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
      const result = res.data.map((data: [any]) => {
        return Object.fromEntries(
          res.column_meta.map((item: [any], index: number) => {
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

    if (typeof err === 'string' && err.includes('Permission denied')) {
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

function loginWithOAuth() {
  // Redirect to OAuth authorization endpoint
  oauthAuthorize();
}

function switchLanguage() {
  if (getLocalLang() == 'zh') {
    /* @ts-expect-error: 属性“value”在类型“string | WritableComputedRef<string, string>”上不存在。 */
    i18n.global.locale.value = 'en';
    localStorage.setItem('local_language', 'en');
    setLocale('en');
  } else {
    /* @ts-expect-error: 属性“value”在类型“string | WritableComputedRef<string, string>”上不存在。 */
    i18n.global.locale.value = 'zh';
    localStorage.setItem('local_language', 'zh');
    setLocale('zh');
  }
  dynamicValidateFormRef.value?.resetFields();
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

  .oauth-button {
    width: 100%;
    font-size: 16px;
    font-weight: 700;
    color: #fff;

    //   color: #606266;
    //   background-color: #fff;
    //   border: 1px solid #dcdfe6;

    //   &:hover {
    //     color: #409eff;
    //     background-color: #ecf5ff;
    //     border-color: #c6e2ff;
    //   }

    //   .oauth-icon {
    //     margin-right: 8px;
    //     vertical-align: middle;
    //   }
  }
}
</style>
