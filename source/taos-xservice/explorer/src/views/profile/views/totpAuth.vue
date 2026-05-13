<template>
  <div class="totp-auth">
    <section class="settings-section">
      <h2 class="section-title">{{ t('profile.totpAuth') }}</h2>

      <!-- Loading state -->
      <div v-if="pageLoading" v-loading="true" style="height: 120px;" />

      <!-- TOTP Enabled state -->
      <template v-else-if="totpEnabled && !showBindFlow">
        <div class="status-card enabled">
          <el-icon class="status-icon" :size="20"><CircleCheckFilled /></el-icon>
          <div class="status-text">
            <span class="status-label">{{ t('profile.totpEnabledStatus') }}</span>
          </div>
        </div>
        <el-button type="danger" plain @click="showDisableDialog = true">
          {{ t('profile.disableTotp') }}
        </el-button>
      </template>

      <!-- TOTP Disabled state -->
      <template v-else-if="!totpEnabled && !showBindFlow">
        <p class="description">{{ t('profile.totpDescription') }}</p>
        <el-button type="primary" :loading="generating" @click="startEnableFlow">
          {{ t('profile.enableTotp') }}
        </el-button>
      </template>

      <!-- Binding Flow -->
      <template v-if="showBindFlow">
        <!-- Step 1: QR Code -->
        <div class="bind-step">
          <h3>{{ t('profile.totpStep1Title') }}</h3>
          <p class="step-desc">{{ t('profile.totpStep1Desc') }}</p>
          <div class="qrcode-container">
            <canvas ref="qrcodeCanvas" />
          </div>
          <div class="manual-entry">
            <span class="manual-label">{{ t('profile.totpManualEntry') }}</span>
            <code class="secret-code">{{ totpSecret }}</code>
          </div>
          <el-alert type="warning" :closable="false" show-icon style="margin: 16px 0;">
            {{ t('profile.totpSecretWarning') }}
          </el-alert>
        </div>

        <!-- Step 2: Verify -->
        <div class="bind-step">
          <h3>{{ t('profile.totpStep2Title') }}</h3>
          <p class="step-desc">{{ t('profile.totpStep2Desc') }}</p>
          <div class="verify-form">
            <el-input
              v-model="verifyPassword"
              type="password"
              :placeholder="t('profile.passwordPlaceholder')"
              style="width: 240px;"
              show-password
            />
            <el-input
              v-model="verifyCode"
              :placeholder="t('profile.totpCodePlaceholder')"
              maxlength="6"
              style="width: 240px;"
              @keyup.enter="verifyBinding"
            />
            <el-button type="primary" :loading="verifying" @click="verifyBinding">
              {{ t('profile.totpVerify') }}
            </el-button>
            <el-button @click="cancelBindFlow">
              {{ t('profile.totpCancel') }}
            </el-button>
          </div>
        </div>
      </template>
    </section>

    <!-- Disable TOTP Dialog -->
    <el-dialog
      v-model="showDisableDialog"
      :title="t('profile.totpUnbindConfirm')"
      width="420px"
      :close-on-click-modal="false"
    >
      <p style="margin-bottom: 16px; color: #666;">{{ t('profile.totpUnbindDesc') }}</p>
      <el-input
        v-model="disablePassword"
        type="password"
        :placeholder="t('profile.passwordPlaceholder')"
        style="margin-bottom: 12px;"
        show-password
      />
      <el-input
        v-model="disableCode"
        :placeholder="t('profile.totpCodePlaceholder')"
        maxlength="6"
        @keyup.enter="confirmDisable"
      />
      <template #footer>
        <el-button @click="showDisableDialog = false">{{ t('profile.cancel') }}</el-button>
        <el-button type="danger" :loading="disabling" @click="confirmDisable">
          {{ t('profile.confirm') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { CircleCheckFilled } from '@element-plus/icons-vue';
import QRCode from 'qrcode';
import { sendSQLReq } from '@/api/explorer';
import { totpEnable, totpDisable } from '@/api/profile';
import { TimeBasedXor } from '@/utils/timeBasedXor';

const { t } = useI18n();

const username = ref(localStorage.getItem('username') || '');
const pageLoading = ref(true);
const totpEnabled = ref(false);

// Bind flow
const showBindFlow = ref(false);
const generating = ref(false);
const totpSecret = ref('');
const totpUri = ref('');
const qrcodeCanvas = ref<HTMLCanvasElement | null>(null);
const verifyCode = ref('');
const verifyPassword = ref('');
const verifying = ref(false);

// Disable flow
const showDisableDialog = ref(false);
const disableCode = ref('');
const disablePassword = ref('');
const disabling = ref(false);

async function fetchTotpStatus() {
  pageLoading.value = true;
  try {
    const res = await sendSQLReq(
      `SELECT totp FROM information_schema.ins_users WHERE name = '${username.value}'`
    );
    if (res?.code === 0 && res.data?.length > 0) {
      totpEnabled.value = res.data[0][0] === 1 || res.data[0][0] === '1';
    }
  } catch {
    // TOTP may not be supported on older TSDB versions
  } finally {
    pageLoading.value = false;
  }
}

async function startEnableFlow() {
  generating.value = true;
  try {
    const res: any = await totpEnable();
    if (res?.code === 0 && res.data) {
      totpSecret.value = res.data.secret;
      totpUri.value = res.data.uri;
      showBindFlow.value = true;

      // Render QR code after DOM update
      await nextTick();
      if (qrcodeCanvas.value) {
        try {
          await QRCode.toCanvas(qrcodeCanvas.value, totpUri.value, { width: 200 });
        } catch {
          ElMessage.error(t('profile.totpQrcodeFailed'));
        }
      }
    } else {
      ElMessage.error(res?.desc || t('profile.totpGenerateFailed'));
    }
  } catch (err: any) {
    ElMessage.error(err?.desc || err?.message || t('profile.totpGenerateFailed'));
  } finally {
    generating.value = false;
  }
}

function cancelBindFlow() {
  showBindFlow.value = false;
  totpSecret.value = '';
  totpUri.value = '';
  verifyCode.value = '';
  verifyPassword.value = '';
}

async function verifyBinding() {
  const code = verifyCode.value.trim();
  if (!code || code.length !== 6) {
    ElMessage.warning(t('profile.totpCodePlaceholder'));
    return;
  }
  if (!verifyPassword.value) {
    ElMessage.warning(t('profile.passwordPlaceholder'));
    return;
  }
  verifying.value = true;
  try {
    const res: any = await totpEnable(code, new TimeBasedXor(300).encrypt(verifyPassword.value));
    if (res?.code === 0) {
      ElMessage.success(t('profile.totpBindSuccess'));
      showBindFlow.value = false;
      totpEnabled.value = true;
      totpSecret.value = '';
      totpUri.value = '';
      verifyCode.value = '';
      verifyPassword.value = '';
    } else {
      ElMessage.error(res?.desc || t('profile.totpVerifyFailed'));
    }
  } catch (err: any) {
    ElMessage.error(err?.desc || err?.message || t('profile.totpVerifyFailed'));
  } finally {
    verifying.value = false;
  }
}

async function confirmDisable() {
  const code = disableCode.value.trim();
  if (!code || code.length !== 6) {
    ElMessage.warning(t('profile.totpCodePlaceholder'));
    return;
  }
  if (!disablePassword.value) {
    ElMessage.warning(t('profile.passwordPlaceholder'));
    return;
  }
  disabling.value = true;
  try {
    const res: any = await totpDisable(code, new TimeBasedXor(300).encrypt(disablePassword.value));
    if (res?.code === 0) {
      ElMessage.success(t('profile.totpUnbindSuccess'));
      showDisableDialog.value = false;
      totpEnabled.value = false;
      disableCode.value = '';
      disablePassword.value = '';
    } else {
      ElMessage.error(res?.desc || t('profile.totpVerifyFailed'));
    }
  } catch (err: any) {
    ElMessage.error(err?.desc || err?.message || t('profile.totpVerifyFailed'));
  } finally {
    disabling.value = false;
  }
}

onMounted(() => {
  fetchTotpStatus();
});
</script>

<style lang="scss" scoped>
.settings-section {
  max-width: 680px;
}

.section-title {
  font-size: 20px;
  font-weight: 600;
  color: #1f2328;
  padding-bottom: 12px;
  border-bottom: 1px solid #d1d9e0;
  margin: 0 0 16px 0;
}

.description {
  color: #636c76;
  font-size: 14px;
  line-height: 1.6;
  margin-bottom: 20px;
}

.status-card {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 16px;
  border-radius: 6px;
  margin-bottom: 16px;

  &.enabled {
    background: #dafbe1;
    border: 1px solid #aceebb;
  }

  .status-icon {
    color: #1a7f37;
  }

  .status-label {
    font-size: 14px;
    color: #1f2328;
  }
}

.bind-step {
  margin-bottom: 24px;

  h3 {
    font-size: 16px;
    font-weight: 600;
    color: #1f2328;
    margin: 0 0 8px 0;
  }

  .step-desc {
    color: #636c76;
    font-size: 14px;
    margin-bottom: 16px;
  }
}

.qrcode-container {
  display: inline-block;
  padding: 12px;
  background: #fff;
  border: 1px solid #d1d9e0;
  border-radius: 8px;
  margin-bottom: 12px;
}

.manual-entry {
  display: flex;
  flex-direction: column;
  gap: 4px;

  .manual-label {
    font-size: 13px;
    color: #636c76;
  }

  .secret-code {
    font-family: 'SF Mono', SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace;
    font-size: 14px;
    background: #f6f8fa;
    padding: 8px 12px;
    border-radius: 6px;
    border: 1px solid #d1d9e0;
    word-break: break-all;
    user-select: all;
  }
}

.verify-form {
  display: flex;
  align-items: center;
  gap: 12px;
}
</style>
