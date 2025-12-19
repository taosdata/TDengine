<template>
  <div class="avatar_wrapper">
    <el-dropdown trigger="hover" placement="bottom">
      <div class="avatar_block">
        <span>{{ user }}</span>
      </div>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item>
            <router-link class="drop-block" to="/profile">
              <Icon name="profile" class="dropdown_icon"></Icon>
              {{ $t('login.profile') }}
            </router-link>
          </el-dropdown-item>
          <div class="custom-divider"></div>
          <el-dropdown-item>
            <div class="drop-block" @click="logout">
              <!-- 图标有问题，需特殊处理 -->
              <Icon name="signout" class="dropdown_icon" style="width: 20px; height: 20px"></Icon>
              <span style="color: #4259ce">{{ $t('signOut') }}</span>
            </div>
          </el-dropdown-item>
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<script setup lang="ts">
import Icon from '@/components/Icon/index.vue';
import { useStore } from 'vuex';

const store = useStore();
const router = useRouter();

const user = computed(() => {
  return store.state.app.userInfo?.lastname?.trim()?.slice(0, 1)?.toUpperCase() || 'T';
});

function clearLocalStorage() {
  const lang = localStorage.getItem('local_language');
  const disturbTimeout = localStorage.getItem('modalLastCheckedTime');
  localStorage.clear();
  lang && localStorage.setItem('local_language', lang);
  disturbTimeout && localStorage.setItem('modalLastCheckedTime', disturbTimeout);
}

async function logout() {
  clearLocalStorage();

  await store.dispatch('app/logout');
  // window.location.reload();
}
</script>

<style lang="scss" scoped>
.avatar_wrapper {
  cursor: pointer;
}

.avatar_block {
  margin-top: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  border: 1px solid $color-primary;
  border-radius: 50%;
  color: $color-primary;
}
.avatar_svg {
  width: 26px;
  height: 26px;
}
.drop-block {
  display: flex;
  align-items: center;
  padding: 6px 0;
}
.dropdown_icon {
  width: 20px;
  height: 20px;
  margin-right: 8px;
}
.custom-divider {
  display: none;
}
</style>
