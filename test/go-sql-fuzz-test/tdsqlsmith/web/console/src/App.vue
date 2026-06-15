<script setup lang="ts">
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from './stores/auth'
import { Document, SwitchButton } from '@element-plus/icons-vue'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const showShell = computed(() => route.name !== 'login')
const activeMenu = computed(() => {
  if (String(route.name) === 'report-detail') {
    return '/reports'
  }
  return route.path
})

function toReports() {
  router.push('/reports')
}

function logout() {
  auth.clearToken()
  router.replace('/login')
}
</script>

<template>
  <el-config-provider>
    <div class="app-background"></div>
    <el-container v-if="showShell" class="shell-root">
      <el-aside class="shell-aside" width="268px">
        <div class="brand-head">
          <p class="brand-eyebrow">tdsqlsmith</p>
          <h1 class="brand-title">Crash Console</h1>
        </div>

        <el-menu
          class="nav-menu"
          :default-active="activeMenu"
          :router="false"
          @select="toReports"
        >
          <el-menu-item index="/reports">
            <el-icon><Document /></el-icon>
            <span>Reports</span>
          </el-menu-item>
        </el-menu>

        <div class="aside-footer">
          <el-button class="logout-btn" type="danger" plain @click="logout">
            <el-icon><SwitchButton /></el-icon>
            Logout
          </el-button>
        </div>
      </el-aside>

      <el-main class="shell-main">
        <RouterView />
      </el-main>
    </el-container>

    <el-container v-else class="login-root">
      <el-main class="login-main">
        <RouterView />
      </el-main>
    </el-container>
  </el-config-provider>
</template>
