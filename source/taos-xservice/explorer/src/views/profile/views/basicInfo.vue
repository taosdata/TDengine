<template>
  <div class="basic-info">
    <section class="settings-section">
      <h2 class="section-title">{{ t('profile.basicInfo') }}</h2>
      <div class="info-card">
        <div class="info-row">
          <span class="info-label">{{ t('profile.username') }}</span>
          <span class="info-value">{{ username }}</span>
        </div>
        <div class="info-row">
          <span class="info-label">{{ t('profile.totpStatus') }}</span>
          <span class="info-value">
            <el-tag v-if="totpEnabled" type="success" size="small" round>{{ t('profile.totpEnabled') }}</el-tag>
            <el-tag v-else type="info" size="small" round>{{ t('profile.totpDisabled') }}</el-tag>
          </span>
        </div>
        <div class="info-row last">
          <span class="info-label">{{ t('profile.tokenCount') }}</span>
          <span class="info-value">{{ tokenCount }}</span>
        </div>
      </div>
    </section>

    <section class="settings-section">
      <h2 class="section-title">{{ t('profile.changePassword') }}</h2>
      <div class="password-card">
        <ChangePass class="form-style" :class="{ edit: isPassword }" @close="isPassword = false" />
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import ChangePass from '@/views/profile/components/changePassword.vue';
import { sendSQLReq } from '@/api/explorer';

const { t } = useI18n();
const isPassword = ref<boolean>(false);

const username = ref(localStorage.getItem('username') || '');
const totpEnabled = ref(false);
const tokenCount = ref(0);

async function fetchUserInfo() {
  try {
    const totpRes = await sendSQLReq(`SELECT totp FROM information_schema.ins_users WHERE name = '${username.value}'`);
    if (totpRes?.code === 0 && totpRes.data?.length > 0) {
      totpEnabled.value = totpRes.data[0][0] === 1 || totpRes.data[0][0] === '1';
    }
  } catch (e) {
    // ignore — TOTP may not be supported on older TSDB versions
  }

  try {
    const tokenRes = await sendSQLReq(
      `SELECT count(*) FROM information_schema.ins_tokens WHERE \`user\` = '${username.value}' AND \`extra_info\` NOT LIKE '__auto__'`
    );
    if (tokenRes?.code === 0 && tokenRes.data?.length > 0) {
      tokenCount.value = Number(tokenRes.data[0][0]) || 0;
    }
  } catch (e) {
    // ignore — Token may not be supported on older TSDB versions
  }
}

onMounted(() => {
  fetchUserInfo();
});
</script>

<style lang="scss" scoped>
.basic-info {
  max-width: 680px;

  &:deep(.el-form-item) {
    margin-bottom: 10px;
  }

  &:deep(.edit .el-form-item) {
    margin-bottom: 20px;
  }

  &:deep(.el-input.is-disabled .el-input__inner) {
    color: #16191f;
  }
}

.settings-section {
  margin-bottom: 32px;
}

.section-title {
  font-size: 20px;
  font-weight: 600;
  color: #1f2328;
  padding-bottom: 12px;
  border-bottom: 1px solid #d1d9e0;
  margin: 0 0 16px 0;
}

.info-card {
  background: #f6f8fa;
  border: 1px solid #d1d9e0;
  border-radius: 6px;
  overflow: hidden;
}

.info-row {
  display: flex;
  align-items: center;
  padding: 12px 16px;
  border-bottom: 1px solid #d1d9e0;

  &.last {
    border-bottom: none;
  }
}

.info-label {
  width: 140px;
  flex-shrink: 0;
  font-size: 14px;
  font-weight: 500;
  color: #1f2328;
}

.info-value {
  font-size: 14px;
  color: #636c76;
}

.password-card {
  max-width: 480px;
}

.form-style {
  &:deep(.el-form-item__label) {
    font-weight: 500;
  }
}
</style>
