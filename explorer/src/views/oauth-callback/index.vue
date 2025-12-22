<template>
  <div class="oauth-callback">
    <div class="callback-content">
      <div v-if="loading" class="loading-container">
        <el-icon class="is-loading" :size="50">
          <Loading />
        </el-icon>
        <p class="loading-text">{{ $t('login.oauthProcessing') }}</p>
      </div>
      <div v-else-if="error" class="error-container">
        <el-icon :size="50">
          <CircleClose />
        </el-icon>
        <p class="error-text">{{ errorMessage }}</p>
        <el-button type="primary" @click="returnToLogin">{{
          $t('login.returnToLogin') || 'Return to Login'
        }}</el-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { t } from '@/lang';
import { ref, onMounted } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import { useStore } from 'vuex';
import { ElMessage } from 'element-plus';
import { Loading, CircleClose } from '@element-plus/icons-vue';
import { oauthMe } from '@/api/oauth';

const router = useRouter();
const route = useRoute();
const store = useStore();

const loading = ref(true);
const error = ref(false);
const errorMessage = ref('');
console.log(location);
onMounted(async () => {
  try {
    // If IdP returned an error, show it
    const errorParam = route.query.error as string;
    if (errorParam) {
      error.value = true;
      errorMessage.value = decodeURIComponent(errorParam);
      loading.value = false;
      ElMessage.error(errorMessage.value);
      return;
    }

    // Verify session by calling profile endpoint and including cookies.
    // Use fetch with credentials: 'include' to ensure the httpOnly session cookie is sent.
    const profileResp = await oauthMe();

    if (!profileResp.user_id) {
      // Not authenticated or other error
      if (profileResp.status === 401) {
        error.value = true;
        errorMessage.value = t('login.oauthLoginError') || 'Not authenticated';
      } else {
        let bodyText = '';
        try {
          bodyText = await profileResp.text();
        } catch (e) {
          bodyText = '';
        }
        error.value = true;
        errorMessage.value = bodyText || 'Failed to verify OAuth session';
      }
      loading.value = false;
      return;
    }

    // Session is valid. Mark app as OAuth-logged-in.
    await store.dispatch('app/setOAuthLogin', true);

    // Show success message
    ElMessage.success(t('login.oauthLoginSuccess'));

    // Redirect to home page after a short delay
    setTimeout(() => {
      router.push({ path: '/explorer' });
    }, 500);
  } catch (err: any) {
    console.error('OAuth callback error:', err);
    error.value = true;
    errorMessage.value = err.message || t('login.oauthLoginError');
    loading.value = false;
  }
});

async function returnToLogin() {
  // Do not manipulate oauth token in localStorage.
  // Instead, attempt to clear server-side OAuth session and reset app state.
  try {
    // Inform backend to clear session cookie (if endpoint available).
    await fetch('/api/-/oauth/logout', {
      method: 'POST',
      credentials: 'include'
    });
  } catch (e) {
    // Ignore errors; proceed to client-side cleanup.
    console.warn('OAuth logout request failed', e);
  }

  // Clear any client-side OAuth flags in store (do not persist tokens in localStorage).
  try {
    await store.dispatch('app/setOAuthLogin', false);
  } catch (e) {
    // ignore
  }

  router.push({ path: '/login' });
}
</script>

<style scoped lang="scss">
.oauth-callback {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;

  // background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.callback-content {
  min-width: 400px;
  padding: 60px 80px;
  text-align: center;
  background: white;
  border-radius: 12px;
  box-shadow: 0 10px 40px rgb(0 0 0 / 10%);
}

.loading-container,
.error-container {
  display: flex;
  flex-direction: column;
  gap: 24px;
  align-items: center;
}

.loading-text,
.error-text {
  margin: 0;
  font-size: 16px;
  color: #606266;
}

.error-text {
  max-width: 400px;
  color: #f56c6c;
  word-wrap: break-word;
}

.el-button {
  margin-top: 16px;
}
</style>
