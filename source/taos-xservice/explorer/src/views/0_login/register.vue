<template>
  <div v-loading="pageLoading" class="login">
    <section :class="['content', { 'content-registered': false }]">
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

      <div class="login-content register-box">
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
            <el-form-item v-if="!isLocaleLanguageEn" prop="username">
              <p class="label-form">
                <span>{{ $t('register.name') }}</span>
              </p>
              <el-input
                ref="name"
                v-model="registerValidateForm.name"
                :placeholder="$t('register.nameTips')"
              ></el-input>
            </el-form-item>
            <div v-else style="display: flex; justify-content: space-between">
              <el-form-item prop="firstname" style="width: 49%">
                <p class="label-form">
                  <span>{{ $t('register.firstName') }}</span>
                </p>
                <el-input
                  ref="firstname"
                  v-model="registerValidateForm.firstname"
                  :placeholder="$t('register.firstnameTips')"
                ></el-input>
              </el-form-item>
              <el-form-item prop="lastname" style="width: 49%">
                <p class="label-form">
                  <span>{{ $t('register.lastName') }}</span>
                </p>
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
            <el-form-item prop="phoneEmailRef">
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
            <el-form-item prop="verification_code">
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
import { getLocalLang } from '@/utils/index';
import { FormInstance } from 'element-plus';
import dataJson from './data.json';
import { getUrls, fetchIsbinding, fetchVerificationCode, getVerificationResult, fetchCaptcha } from '@/api/login';

import { useRouter } from 'vue-router';
import { useStore } from 'vuex';
import i18n from '@/lang';
import { setLocale } from 'taos-ui/config';
const { t } = useI18n();
const store = useStore();
const router = useRouter();
const { $IS_COMMUNITY, $IS_TSDBLITE, $IS_OEM, $error } = inject('globalCustomProperties') as GlobalCustomProperties;
const usernameRef = ref<HTMLElement | null>();
const phoneEmailRef = ref<HTMLElement | null>();
const captchaRef = ref<HTMLElement | null>();
const dynamicValidateFormRef = ref<FormInstance>();
const captchaFormRef = ref<FormInstance>();
const registerValidateFormRef = ref<FormInstance>();

const validatePass = (_rule: any, value: string, callback: (arg0?: Error | undefined) => void) => {
  if (value === '') {
    callback(new Error(t('login.passwordTips')));
  } else {
    callback();
  }
};
const validatePhoneEmail = (_rule: any, value: string, callback: (arg0?: Error | undefined) => void) => {
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
const ts = ref<number>();
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
const registered = ref<boolean>(false); // for test
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

async function init() {
  await getClusterAndDashboardUrl();
  localStorage.setItem('supportWebsite', dataJson.supportWebsite);
  localStorage.setItem('documentWebsite', dataJson.documentWebsite);
  console.log('IS_TSDBLITE', $IS_TSDBLITE);
  if ($IS_COMMUNITY && !$IS_TSDBLITE) {
    await getIsbinding();
  }
}
init();
onMounted(() => {
  usernameRef.value?.focus();
  nextTick(() => {
    if (import.meta.env.VITE_APP_CUS_NAME && import.meta.env.VITE_APP_CUS_NAME !== 'TDengine') {
      const dynamic: HTMLElement = document.querySelector('.dynamic-title') as HTMLElement;
      dynamic.innerText = import.meta.env.VITE_APP_CUS_NAME + ' Management System';
    }
  });
});

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
function checkPhone(val: string) {
  return /^1[3456789]\d{9}$/.test(val);
}
function checkEmail(val: string) {
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
  const result = await fetchCaptcha(registerValidateForm.phone_email, ts.value);

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
  formEl.validate(async (valid: boolean) => {
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
            // 如果校验通过，则注册成功，跳转到 Explorer 页面
            sessionStorage.setItem('registerKey', formData.phone_email);
            setTimeout(() => {
              pageLoading.value = false;
              ElMessage.success(t('register.success.registerSuccess'));
            }, 1000);

            await router.push({ path: '/explorer' });

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
      return;
    }
  });
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
  buttonTextOfGetVerificationCode.value = t('register.getVerificationCode');

  dynamicValidateFormRef.value?.resetFields();
  registerValidateFormRef.value?.resetFields();
  formRules.username[0].message = t('login.usernameTips');
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
