<template>
  <el-dialog
    v-model="visible"
    align="center"
    :title="$t('taoscluster.addxnodes')"
    width="600px"
    :destroy-on-close="true"
    :close-on-click-modal="false"
    @close="handleClose"
  >
    <el-form ref="formRef" :model="form" :rules="rules" label-width="auto" autocomplete="off" @submit.prevent>
      <el-form-item :label="$t('taoscluster.endpoint')" prop="endpoint" required>
        <el-input v-model.trim="form.endpoint" autocomplete="off" @keyup.enter="handleSubmit"></el-input>
      </el-form-item>
      <el-tabs v-model="authMode">
        <el-tab-pane :label="$t('taoscluster.authUserPassTab')" name="credentials">
          <el-form-item :label="$t('taoscluster.user')" prop="user">
            <el-input v-model.trim="form.user" autocomplete="off"></el-input>
          </el-form-item>
          <el-form-item :label="$t('taoscluster.password')" prop="pass">
            <el-input v-model.trim="form.pass" autocomplete="new-password" show-password @keyup.enter="handleSubmit"></el-input>
          </el-form-item>
        </el-tab-pane>
        <el-tab-pane :label="$t('taoscluster.authTokenTab')" name="token">
          <el-form-item :label="$t('taoscluster.token')" prop="token">
            <el-input v-model.trim="form.token" autocomplete="new-password" show-password @keyup.enter="handleSubmit"></el-input>
          </el-form-item>
        </el-tab-pane>
      </el-tabs>
    </el-form>

    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button class="w100" @click="visible = false">{{ $t('cancel') }}</el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button class="w100" type="primary" @click="handleSubmit">{{ $t('confirm') }}</el-button>
      </el-col>
    </el-row>
  </el-dialog>
</template>

<script setup lang="ts">
import type { FormInstance, FormRules } from 'element-plus';
import { buildCreateXnodeSql, type XnodeFormState, validateXnodeForm } from './xnodeDialog.helper';

const props = defineProps<{
  modelValue: boolean;
  sendSql: (sql: string) => Promise<{ code: number | string; desc?: string; message?: string }>;
}>();

const emit = defineEmits<{
  'update:modelValue': [boolean];
  success: [];
}>();

const { t } = useI18n();
const formRef = ref<FormInstance>();
const authMode = ref<'credentials' | 'token'>('credentials');
const visible = computed({
  get: () => props.modelValue,
  set: value => emit('update:modelValue', value)
});

const form = reactive<XnodeFormState>({
  endpoint: '',
  user: '',
  pass: '',
  token: ''
});

watch(authMode, mode => {
  if (mode === 'credentials') {
    form.token = '';
    formRef.value?.clearValidate(['token']);
    return;
  }
  form.user = '';
  form.pass = '';
  formRef.value?.clearValidate(['user', 'pass']);
});

function validateUser(_rule: unknown, _value: string, callback: (error?: Error) => void) {
  const validation = validateXnodeForm(form);
  if (validation === 'authMode') {
    callback(new Error(t('taoscluster.xnodeAuthModeExclusive')));
    return;
  }
  if (validation === 'credentials') {
    callback(new Error(t('taoscluster.userPassRequired')));
    return;
  }
  if (validation === 'user') {
    callback(new Error(t('taoscluster.invalidUser')));
    return;
  }
  callback();
}

function validatePass(_rule: unknown, _value: string, callback: (error?: Error) => void) {
  const validation = validateXnodeForm(form);
  if (validation === 'authMode') {
    callback(new Error(t('taoscluster.xnodeAuthModeExclusive')));
    return;
  }
  if (validation === 'credentials') {
    callback(new Error(t('taoscluster.userPassRequired')));
    return;
  }
  callback();
}

function validateToken(_rule: unknown, _value: string, callback: (error?: Error) => void) {
  const validation = validateXnodeForm(form);
  if (validation === 'authMode') {
    callback(new Error(t('taoscluster.xnodeAuthModeExclusive')));
    return;
  }
  callback();
}

const rules = reactive<FormRules<XnodeFormState>>({
  endpoint: [{ required: true, message: t('taoscluster.endpointRequired') }],
  user: [{ validator: validateUser, trigger: 'blur' }],
  pass: [{ validator: validatePass, trigger: 'blur' }],
  token: [{ validator: validateToken, trigger: 'blur' }]
});

function resetForm() {
  authMode.value = 'credentials';
  form.endpoint = '';
  form.user = '';
  form.pass = '';
  form.token = '';
  formRef.value?.clearValidate();
}

function handleClose() {
  resetForm();
}

async function handleSubmit() {
  if (!formRef.value) return;
  const valid = await formRef.value.validate().catch(() => false);
  if (!valid) return;

  let result: Awaited<ReturnType<typeof props.sendSql>>;
  try {
    result = await props.sendSql(buildCreateXnodeSql(form));
  } catch (error: any) {
    ElMessage.error(error?.desc || error?.message || t('taoscluster.createXnodeFailed'));
    return;
  }
  if (result.code != 0) {
    ElMessage.error(result.desc || result.message || t('taoscluster.createXnodeFailed'));
    return;
  }

  emit('success');
  visible.value = false;
}
</script>
