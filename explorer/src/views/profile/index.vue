<template>
  <div class="profile-page">
    <aside class="profile-sidebar">
      <div class="sidebar-user">
        <div class="user-avatar">{{ username.charAt(0).toUpperCase() }}</div>
        <div class="user-meta">
          <div class="user-name">{{ username }}</div>
        </div>
      </div>
      <nav class="sidebar-nav">
        <a
          v-for="item in navItems"
          :key="item.name"
          class="nav-item"
          :class="{ active: activeName === item.name }"
          @click="activeName = item.name"
        >
          <el-icon class="nav-icon"><component :is="item.icon" /></el-icon>
          <span>{{ item.label }}</span>
        </a>
      </nav>
    </aside>
    <main class="profile-content">
      <BasicInfo v-if="activeName === 'basic'" />
      <TotpAuth v-else-if="activeName === 'totp'" />
      <TokenManagement v-else-if="activeName === 'tokens'" />
    </main>
  </div>
</template>

<script setup lang="ts">
import { User, Lock, Key } from '@element-plus/icons-vue';
import BasicInfo from './views/basicInfo.vue';
import TotpAuth from './views/totpAuth.vue';
import TokenManagement from './views/tokenManagement.vue';

const { t } = useI18n();
const router = useRouter();
const route = useRoute();

const username = ref(localStorage.getItem('username') || '');
const activeName: Ref<string> = ref('basic');

const navItems = computed(() => [
  { name: 'basic', label: t('profile.basicInfo'), icon: User },
  { name: 'totp', label: t('profile.totpAuth'), icon: Lock },
  { name: 'tokens', label: t('profile.tokenManagement'), icon: Key },
]);

watch(
  () => route,
  (val: any) => {
    activeName.value = val.name;
  },
  {
    deep: true
  }
);

watch(
  activeName,
  val => {
    router.push('/profile/' + val);
  },
  {
    deep: true
  }
);
</script>

<style scoped lang="scss">
.profile-page {
  display: flex;
  height: 100%;
  background-color: #fff;
  border: 1px solid #e3e4e6;
  border-radius: 4px;
}

.profile-sidebar {
  width: 240px;
  flex-shrink: 0;
  border-right: 1px solid #e3e4e6;
  padding: 24px 16px;
}

.sidebar-user {
  display: flex;
  align-items: center;
  gap: 12px;
  padding-bottom: 20px;
  margin-bottom: 8px;
  border-bottom: 1px solid #e3e4e6;
}

.user-avatar {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background: linear-gradient(135deg, #4259ce, #6b7ff0);
  color: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 18px;
  font-weight: 600;
  flex-shrink: 0;
}

.user-name {
  font-size: 15px;
  font-weight: 600;
  color: #1f2328;
}

.sidebar-nav {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 12px;
  border-radius: 6px;
  font-size: 14px;
  color: #1f2328;
  cursor: pointer;
  transition: background-color 0.15s;
  text-decoration: none;
  user-select: none;

  &:hover {
    background-color: #eef1f6;
  }

  &.active {
    background-color: #e8ecf4;
    font-weight: 500;

    .nav-icon {
      color: #4259ce;
    }
  }
}

.nav-icon {
  font-size: 16px;
  color: #636c76;
}

.profile-content {
  flex: 1;
  padding: 32px 40px;
  overflow-y: auto;
  min-width: 0;
}
</style>
