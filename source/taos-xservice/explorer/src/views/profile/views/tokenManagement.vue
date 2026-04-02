<template>
  <div class="token-management">
    <section class="settings-section">
      <h2 class="section-title">{{ t('profile.tokenManagement') }}</h2>

      <div class="toolbar">
        <el-button type="primary" @click="openCreateDialog">
          {{ t('profile.createToken') }}
        </el-button>
      </div>

      <el-table :data="tokenList" v-loading="tableLoading" stripe style="width: 100%;">
        <el-table-column prop="name" :label="t('profile.tokenName')" min-width="140" />
        <el-table-column :label="t('profile.tokenEnabled')" width="90" align="center">
          <template #default="{ row }">
            <el-switch
              :model-value="row.enable === 1"
              @change="(val: boolean) => toggleEnable(row, val)"
            />
          </template>
        </el-table-column>
        <el-table-column :label="t('profile.tokenCreateTime')" min-width="170">
          <template #default="{ row }">
            {{ formatDateTime(row.create_time) }}
          </template>
        </el-table-column>
        <el-table-column :label="t('profile.tokenExpireTime')" min-width="170">
          <template #default="{ row }">
            <span v-if="isNeverExpires(row.expire_time)">{{ t('profile.tokenNeverExpires') }}</span>
            <span v-else :class="{ 'expired-text': isExpired(row.expire_time) }">
              {{ formatDateTime(row.expire_time) }}
              <el-tag v-if="isExpired(row.expire_time)" type="danger" size="small" style="margin-left: 4px;">
                {{ t('profile.tokenExpired') }}
              </el-tag>
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="provider" :label="t('profile.tokenProvider')" width="120" />
        <el-table-column prop="extra_info" :label="t('profile.tokenExtraInfo')" min-width="140" show-overflow-tooltip />
        <el-table-column :label="t('profile.tokenActions')" width="140" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" size="small" @click="openEditDialog(row)">
              {{ t('profile.tokenEdit') }}
            </el-button>
            <el-button link type="danger" size="small" @click="confirmDelete(row)">
              {{ t('profile.tokenDelete') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <!-- Create Token Dialog -->
    <el-dialog v-model="createDialogVisible" :title="t('profile.createToken')" width="480px" :close-on-click-modal="false">
      <el-form ref="createFormRef" :model="createForm" :rules="createRules" label-width="100px">
        <el-form-item :label="t('profile.tokenName')" prop="name">
          <el-input v-model="createForm.name" :placeholder="t('profile.tokenNamePlaceholder')" />
        </el-form-item>
        <el-form-item :label="t('profile.tokenEnabled')">
          <el-switch v-model="createForm.enable" />
        </el-form-item>
        <el-form-item :label="t('profile.tokenTtl')">
          <el-input-number v-model="createForm.ttl" :min="0" :max="36500" />
          <span class="ttl-hint">{{ t('profile.tokenTtlHint') }}</span>
        </el-form-item>
        <el-form-item :label="t('profile.tokenNotes')">
          <el-input v-model="createForm.extra_info" :placeholder="t('profile.tokenNotesPlaceholder')" type="textarea" :rows="2" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createDialogVisible = false">{{ t('profile.cancel') }}</el-button>
        <el-button type="primary" :loading="creating" @click="submitCreate">{{ t('profile.confirm') }}</el-button>
      </template>
    </el-dialog>

    <!-- Edit Token Dialog -->
    <el-dialog v-model="editDialogVisible" :title="t('profile.tokenEdit')" width="480px" :close-on-click-modal="false">
      <el-form :model="editForm" label-width="100px">
        <el-form-item :label="t('profile.tokenName')">
          <el-input :model-value="editForm.name" disabled />
        </el-form-item>
        <el-form-item :label="t('profile.tokenEnabled')">
          <el-switch v-model="editForm.enable" />
        </el-form-item>
        <el-form-item :label="t('profile.tokenTtl')">
          <el-input-number v-model="editForm.ttl" :min="0" :max="36500" />
          <span class="ttl-hint">{{ t('profile.tokenTtlHint') }}</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="editDialogVisible = false">{{ t('profile.cancel') }}</el-button>
        <el-button type="primary" :loading="editing" @click="submitEdit">{{ t('profile.confirm') }}</el-button>
      </template>
    </el-dialog>

    <!-- Token Display Dialog (shown once after creation) -->
    <el-dialog v-model="tokenDisplayVisible" :title="t('profile.createToken')" width="520px" :close-on-click-modal="false" :close-on-press-escape="false">
      <el-alert type="warning" :closable="false" show-icon style="margin-bottom: 16px;">
        {{ t('profile.tokenCreateWarning') }}
      </el-alert>
      <div class="token-display">
        <code class="token-value">{{ createdTokenValue }}</code>
        <el-button type="primary" size="small" @click="copyToken">{{ t('profile.tokenCopy') }}</el-button>
      </div>
      <template #footer>
        <el-button type="primary" @click="closeTokenDisplay">{{ t('profile.close') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { parseTime } from '@/utils';
import type { FormInstance } from 'element-plus';

const { t } = useI18n();

const username = ref(localStorage.getItem('username') || '');
const tableLoading = ref(false);
const tokenList = ref<any[]>([]);

// Create dialog
const createDialogVisible = ref(false);
const createFormRef = ref<FormInstance>();
const creating = ref(false);
const createForm = reactive({
  name: '',
  enable: true,
  ttl: 0,
  extra_info: '',
});
const createRules = reactive({
  name: [{ required: true, message: () => t('profile.tokenNameRequired'), trigger: 'blur' }],
});

// Edit dialog
const editDialogVisible = ref(false);
const editing = ref(false);
const editForm = reactive({
  name: '',
  enable: true,
  ttl: 0,
});

// Token display after creation
const tokenDisplayVisible = ref(false);
const createdTokenValue = ref('');

async function fetchTokens() {
  tableLoading.value = true;
  try {
    const res = await sendSQLReq(
      `SELECT * FROM information_schema.ins_tokens WHERE \`user\` = '${username.value}' AND name NOT LIKE '__taosx_%__'`,
      true
    );
    if (Array.isArray(res)) {
      tokenList.value = res;
    } else {
      tokenList.value = [];
    }
  } catch {
    tokenList.value = [];
  } finally {
    tableLoading.value = false;
  }
}

function isNeverExpires(expireTime: string): boolean {
  if (!expireTime) return true;
  return expireTime.includes('1970-01-01');
}

function isExpired(expireTime: string): boolean {
  if (isNeverExpires(expireTime)) return false;
  return new Date(expireTime) < new Date();
}

function formatDateTime(value: string): string {
  if (!value) return '';
  return parseTime(value, 'YYYY-MM-DD HH:mm:ss');
}

async function toggleEnable(row: any, val: boolean) {
  try {
    await sendSQLReq(`ALTER TOKEN ${row.name} ENABLE ${val ? 1 : 0}`);
    row.enable = val ? 1 : 0;
  } catch {
    // revert on failure — refresh list
    fetchTokens();
  }
}

function openCreateDialog() {
  createForm.name = '';
  createForm.enable = true;
  createForm.ttl = 0;
  createForm.extra_info = '';
  createDialogVisible.value = true;
}

async function submitCreate() {
  try {
    await createFormRef.value?.validate();
  } catch {
    return;
  }
  creating.value = true;
  try {
    const enable = createForm.enable ? 1 : 0;
    const extraInfo = createForm.extra_info.replace(/'/g, "''");
    const sql = `CREATE TOKEN IF NOT EXISTS ${createForm.name} FROM USER ${username.value} ENABLE ${enable} PROVIDER 'explorer' TTL ${createForm.ttl} EXTRA_INFO '${extraInfo}'`;
    const res = await sendSQLReq(sql, false, false);
    if (res?.code === 0 && res.data?.length > 0) {
      createdTokenValue.value = res.data[0][0] || '';
      createDialogVisible.value = false;
      tokenDisplayVisible.value = true;
      ElMessage.success(t('profile.tokenCreateSuccess'));
      fetchTokens();
    } else {
      const desc = res?.desc || '';
      if (desc.toLowerCase().includes('token') && desc.toLowerCase().includes('limit')) {
        ElMessage.error(t('profile.tokenLimitReached'));
      } else {
        ElMessage.error(desc || 'Failed to create token');
      }
    }
  } catch (err: any) {
    const desc = err?.desc || err?.message || '';
    if (desc.toLowerCase().includes('limit') || desc.toLowerCase().includes('allow_token_num')) {
      ElMessage.error(t('profile.tokenLimitReached'));
    } else {
      ElMessage.error(desc || 'Failed to create token');
    }
  } finally {
    creating.value = false;
  }
}

function openEditDialog(row: any) {
  editForm.name = row.name;
  editForm.enable = row.enable === 1;
  editForm.ttl = 0;
  editDialogVisible.value = true;
}

async function submitEdit() {
  editing.value = true;
  try {
    const enable = editForm.enable ? 1 : 0;
    await sendSQLReq(`ALTER TOKEN ${editForm.name} ENABLE ${enable} TTL ${editForm.ttl}`);
    ElMessage.success(t('profile.tokenEditSuccess'));
    editDialogVisible.value = false;
    fetchTokens();
  } catch (err: any) {
    ElMessage.error(err?.desc || err?.message || 'Failed to update token');
  } finally {
    editing.value = false;
  }
}

function confirmDelete(row: any) {
  ElMessageBox.confirm(
    t('profile.tokenDeleteConfirm', { name: row.name }),
    t('profile.tokenDelete'),
    { type: 'warning', confirmButtonClass: 'el-button--danger' }
  ).then(async () => {
    try {
      await sendSQLReq(`DROP TOKEN IF EXISTS ${row.name}`);
      ElMessage.success(t('profile.tokenDeleteSuccess'));
      fetchTokens();
    } catch (err: any) {
      ElMessage.error(err?.desc || err?.message || 'Failed to delete token');
    }
  }).catch(() => {});
}

function copyToken() {
  navigator.clipboard.writeText(createdTokenValue.value).then(() => {
    ElMessage.success(t('profile.tokenCopied'));
  });
}

function closeTokenDisplay() {
  tokenDisplayVisible.value = false;
  createdTokenValue.value = '';
}

onMounted(() => {
  fetchTokens();
});
</script>

<style lang="scss" scoped>
.settings-section {
  max-width: 960px;
}

.section-title {
  font-size: 20px;
  font-weight: 600;
  color: #1f2328;
  padding-bottom: 12px;
  border-bottom: 1px solid #d1d9e0;
  margin: 0 0 16px 0;
}

.toolbar {
  margin-bottom: 16px;
}

.expired-text {
  color: #cf222e;
}

.ttl-hint {
  margin-left: 8px;
  font-size: 12px;
  color: #8b949e;
}

.token-display {
  display: flex;
  align-items: center;
  gap: 12px;

  .token-value {
    flex: 1;
    font-family: 'SF Mono', SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace;
    font-size: 13px;
    background: #f6f8fa;
    padding: 10px 12px;
    border-radius: 6px;
    border: 1px solid #d1d9e0;
    word-break: break-all;
    user-select: all;
  }
}
</style>
