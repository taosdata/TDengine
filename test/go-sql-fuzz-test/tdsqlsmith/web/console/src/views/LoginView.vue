<!--
Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>

SPDX-License-Identifier: AGPL-3.0-or-later

This program is free software: you can use, redistribute, and/or modify
it under the terms of the GNU Affero General Public License, version 3
or later ("AGPL"), as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { Lock } from '@element-plus/icons-vue'
import { useAuthStore } from '../stores/auth'
import { ApiClient } from '../api'

const router = useRouter()
const auth = useAuthStore()

const loading = ref(false)
const form = reactive({
  token: auth.token,
})

async function submit() {
  const token = String(form.token ?? '').trim()
  if (!token) {
    ElMessage.warning('Please enter API token')
    return
  }

  auth.setToken(token)
  loading.value = true
  try {
    const api = new ApiClient(() => auth.token)
    await api.verifyAuth()
    ElMessage.success('Authentication successful')
    router.replace('/reports')
  } catch (e) {
    auth.clearToken()
    ElMessage.error(e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="login-wrap">
    <el-card class="login-card" shadow="hover">
      <template #header>
        <div class="login-header">
          <h2>Console Login</h2>
          <p>Use <code>--api-token</code> from <code>tdsqlsmith serve</code>.</p>
        </div>
      </template>

      <el-form @submit.prevent>
        <el-form-item>
          <el-input
            v-model="form.token"
            type="password"
            show-password
            size="large"
            placeholder="tdsqlsmith-dev-token"
            @keyup.enter="submit"
          >
            <template #prefix>
              <el-icon><Lock /></el-icon>
            </template>
          </el-input>
        </el-form-item>

        <el-button type="primary" size="large" :loading="loading" @click="submit">
          {{ loading ? 'Verifying' : 'Enter Console' }}
        </el-button>
      </el-form>
    </el-card>
  </div>
</template>
