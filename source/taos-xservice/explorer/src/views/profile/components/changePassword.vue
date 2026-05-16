<template>
  <el-form
    ref="ruleFormRef"
    :model="changeForm"
    :rules="rules"
    :status-icon="true"
    label-position="top"
    label-width="auto"
    size="default"
  >
    <el-form-item :label="t('login.oldPass')" prop="old_password">
      <el-input
        v-model.trim="changeForm.old_password"
        maxlength="255"
        :show-password="true"
        minlength="8"
        :placeholder="t('login.oldPass')"
        @keyup.enter="change"
      ></el-input>
    </el-form-item>
    <el-form-item :label="t('login.newPass')" prop="new_password">
      <el-popover trigger="click" placement="right-end">
        <ol
          v-dompurify-html="t(enableStrongPassword ? 'login.passwordTip' : 'login.passwordNotStrictTip')"
          style="padding-left: 10px; list-style: unset"
        ></ol>
        <template #reference>
          <el-input
            v-model.trim="changeForm.new_password"
            maxlength="255"
            :show-password="true"
            minlength="8"
            :placeholder="t('login.newPass')"
            @keyup.enter="change"
          ></el-input>
        </template>
      </el-popover>
    </el-form-item>
    <el-form-item :label="t('confirmPass')" prop="confirm_password">
      <el-input
        v-model.trim="changeForm.confirm_password"
        maxlength="255"
        minlength="8"
        :show-password="true"
        :placeholder="t('confirmPass')"
        @keyup.enter="change"
      ></el-input>
    </el-form-item>
    <p v-show="err_msg" class="error-text">{{ err_msg }}</p>
    <el-form-item label=" " class="my-btn">
      <el-button type="primary" :disabled="requestIng" :loading="requestIng" @click="change">{{
        t('login.saveChange')
      }}</el-button>
    </el-form-item>
  </el-form>
</template>

<script setup lang="ts">
import { validPassword, validPasswordNotStrict } from '@/utils/validate';
import { getDatabaseVariables } from '@/api/database';
import { sendSQLReq } from '@/api/explorer';
import { decrypt } from '@/utils/index';
import type { FormInstance } from 'element-plus';

const enableStrongPassword = ref(false);

const { t } = useI18n();
const router = useRouter();
const ruleFormRef = ref<FormInstance>();

let changeForm = reactive({
  old_password: '',
  new_password: '',
  confirm_password: ''
});
const err_msg = ref('');
const requestIng = ref(false);

const validateOldPwd = (_: any, value: any, callback: (arg0?: Error | undefined) => void) => {
  if (!value) {
    return callback(new Error(t('login.oldPass') + t('requiredMessage')));
  } else {
    if (value != decrypt(localStorage.getItem('pwd') || '')) {
      return callback(new Error(t('login.oldPassError')));
    } else {
      return callback();
    }
  }
};
const checkPassword = (_: any, value: string, callback: (arg0: Error | undefined) => void) => {
  err_msg.value = '';
  callback(validatePasswordLocal(value) ? undefined : new Error(t('login.passwordError')));
};
const cheakConfirmPassword = (_: any, value: string, callback: (arg0: Error | undefined) => void) => {
  err_msg.value = '';
  callback(value == changeForm.new_password ? undefined : new Error(t('login.twoPassError')));
};

const validatePasswordLocal = (value: string) => {
  if (enableStrongPassword.value) {
    return validPassword(value);
  } else {
    return validPasswordNotStrict(value);
  }
};

const rules = computed(() => {
  return {
    old_password: [
      {
        required: true,
        trigger: 'blur',
        message: t('login.oldPass') + t('requiredMessage')
      },
      { validator: validateOldPwd, trigger: 'blur' }
    ],
    new_password: [
      {
        required: true,
        trigger: 'blur',
        message: t('login.newPass') + t('requiredMessage')
      },
      { validator: checkPassword, trigger: 'blur' }
    ],
    confirm_password: [
      {
        required: true,
        trigger: 'blur',
        message: t('confirmPass') + t('requiredMessage')
      },
      { validator: cheakConfirmPassword, trigger: 'blur' }
    ]
  };
});

function change() {
  if (requestIng.value) return;
  ruleFormRef.value?.validate(async valid => {
    if (valid) {
      requestIng.value = true;
      const username = localStorage.getItem('username');
      await sendSQLReq(`ALTER USER \`${username}\` PASS '${changeForm.new_password}'`)
        .then(res => {
          if (res) {
            changeForm = {
              old_password: '',
              new_password: '',
              confirm_password: ''
            };
            // this.$message.success(t("login.changeSucc"));
            requestIng.value = false;
            localStorage.removeItem('username');
            localStorage.removeItem('pwd');
            ElMessageBox.alert(t('login.changepwdtip'), t('tips'), {
              showCancelButton: false,
              showConfirmButton: true,
              confirmButtonText: t('ok'),
              closeOnClickModal: false,
              showClose: false,
              type: 'success'
            }).then(() => {
              router.push({
                path: '/login'
              });
            });
          }
        })
        .catch(err => {
          err_msg.value = err.desc || err;
        })
        .finally(() => {
          requestIng.value = false;
        });
    }
  });
}

onMounted(async () => {
  const result = await getDatabaseVariables('enableStrongPassword');
  enableStrongPassword.value = result === true || result === 'true' || result === '1';
});
</script>

<style lang="scss" scoped>
.error-text {
  padding: 10px 0;
  font-size: 14px;
  color: #ff4949;
}

.my-btn {
  margin-top: 20px;
}
</style>
