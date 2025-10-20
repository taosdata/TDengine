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
    </section>

    <div v-if="!$IS_OEM" class="copyright">
      <span>{{ $t('login.copyright') }}</span>
    </div>
  </div>
</template>
<script setup lang="ts">
import { DbBase64 } from '../../utils/dbBase64';
import { deleteCookieItem, getLocalLang } from '@/utils/index';
import { sendSQLReq } from '@/api/explorer';
import { FormInstance } from 'element-plus';
import dataJson from './data.json';
import { getUrls, reportTaosdInfo, firstLoginWith } from '@/api/login';
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
const dynamicValidateForm = reactive({
  cluster: '',
  password: '',
  username: ''
});
const trimmedUsername = computed(() => {
  return dynamicValidateForm.username.trim();
});
const trimmedPassword = computed(() => {
  return dynamicValidateForm.password.trim();
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
async function login() {
  const token = 'Basic ' + DbBase64.encode(trimmedUsername.value + ':' + trimmedPassword.value);
  store.commit('app/SET_TOKEN', token);
  localStorage.setItem('username', trimmedUsername.value);
  localStorage.setItem('pwd', encryptedPwd.value);

  store.commit('app/SAVE_LOGIN_INFO', {
    username: trimmedUsername.value,
    pwd: trimmedPassword.value
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
}
</style>
