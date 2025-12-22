<template>
  <div class="dnode-block">
    <div class="flex-end">
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!$IS_COMMUNITY">
        <template #content>
          <span v-dompurify-html="$t('communityTip')"></span>
        </template>
        <el-button
          plain
          type="primary"
          size="default"
          icon="Plus"
          :disabled="!isRoot || $IS_COMMUNITY"
          style="font-size: 14px"
          @click="importDialog = true"
          >{{ $t('import') }}
        </el-button>
      </el-tooltip>
      <el-button
        plain
        type="primary"
        size="default"
        icon="Plus"
        :disabled="!isRoot"
        style="font-size: 14px"
        @click="showDialog"
        >{{ $t('add') }}
      </el-button>
    </div>
    <el-table v-loading="loading" style="margin-top: 20px" :data="usersList" size="small">
      <el-table-column :label="$t('userName')" prop="name" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('taosuser.createtime')" prop="create_time" show-overflow-tooltip></el-table-column>

      <el-table-column :label="$t('taosuser.action')" width="150">
        <template #default="scope">
          <el-switch
            :model-value="scope.row.enable == 1"
            :disabled="$IS_COMMUNITY ? $IS_COMMUNITY : scope.row.super === 1 || !currentUser.super || !isRoot"
            style="--el-switch-on-color: #13ce66; --el-switch-off-color: #dcdfe6"
            @change="changeState(scope.row)"
          >
          </el-switch
          >&nbsp;&nbsp;
          <el-button
            plain
            size="small"
            :disabled="scope.row.super === 1 || !currentUser.super || !isRoot"
            icon="Edit"
            @click="edit(scope.row)"
          ></el-button>
          <el-button
            plain
            size="small"
            :disabled="scope.row.super === 1 || !currentUser.super || !isRoot"
            icon="Delete"
            @click="del(scope.row)"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      class="pagination"
      layout="total, prev, pager, next"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>

    <el-dialog
      v-model="dialog"
      align="center"
      :title="$t('taosuser.adduser')"
      width="700px"
      :close-on-click-modal="false"
    >
      <UserForm v-if="dialog" :status="dialog" user="" @close="closeDialog"></UserForm>
    </el-dialog>

    <el-dialog
      v-model="editDialog"
      align="center"
      :title="$t('taosuser.edituser')"
      width="700px"
      :close-on-click-modal="false"
    >
      <UserForm :user="editUser" :status="editDialog" @close="closeEditDialog"></UserForm>
    </el-dialog>

    <el-dialog
      v-model="importDialog"
      align="center"
      :title="$t('taosuser.importTitle')"
      width="680px"
      :close-on-click-modal="false"
    >
      <ImportInfo v-if="importDialog" @close="closeImportDialog" @refresh="getUserData"></ImportInfo>
    </el-dialog>

    <template v-if="oauthEnabled && isOAuthSyncUsersSupported">
      <title-bar class="mt16" :name="$t('taosuser.oauth2.title')">
        <el-tag type="success" size="small">OAuth</el-tag></title-bar
      >

      <el-alert
        v-if="$IS_COMMUNITY"
        class="mb12"
        type="warning"
        :closable="false"
        :title="tr('taosuser.oauth2.communityHint', 'Community edition: features are for demo only')"
      />

      <p class="desc">
        {{
          tr(
            'taosuser.oauth2.syncDesc',
            'Sync users from the configured OAuth provider (e.g. /sso/oauth2.0/getUsers) into Explorer’s user mapping. This operation is idempotent.'
          )
        }}
      </p>

      <div class="actions">
        <el-button type="primary" :loading="syncing" :disabled="syncing" @click="onSyncClick">
          {{ syncing ? tr('taosuser.oauth2.syncing', 'Syncing…') : tr('taosuser.oauth2.syncNow', 'Sync Users Now') }}
        </el-button>
      </div>

      <el-descriptions v-if="lastResult" class="mt16 wd-small" :column="1" border>
        <el-descriptions-item :label="tr('taosuser.oauth2.imported', 'Imported')">
          {{ lastResult.imported }}
        </el-descriptions-item>
        <el-descriptions-item :label="tr('taosuser.oauth2.updated', 'Updated')">
          {{ lastResult.updated }}
        </el-descriptions-item>
        <el-descriptions-item :label="tr('taosuser.oauth2.skipped', 'Skipped')">
          {{ lastResult.skipped }}
        </el-descriptions-item>
        <el-descriptions-item v-if="lastResult.message" :label="tr('taosuser.oauth2.message', 'Message')">
          {{ lastResult.message }}
        </el-descriptions-item>
      </el-descriptions>
    </template>
    <template v-if="oauthEnabled">
      <title-bar class="mt16" :name="tr('taosuser.oauth2.existingUsers', 'OAuth User List')">
        <el-tag type="info" size="small">OAuth</el-tag>
      </title-bar>

      <el-table
        v-loading="existLoading"
        class="mt12"
        size="small"
        :data="existingUsers"
        style="margin-top: 12px"
        empty-text="—"
      >
        <el-table-column :label="tr('userName', 'Username')" prop="username" show-overflow-tooltip />
        <el-table-column :label="tr('common.email', 'Email')" prop="email" show-overflow-tooltip />
        <el-table-column :label="`TSDB ${tr('userName', 'Username')}`" prop="tsdb_username" show-overflow-tooltip />
        <el-table-column :label="tr('taosuser.createtime', 'Created At')" prop="created_at" show-overflow-tooltip />
        <el-table-column :label="tr('taosuser.updatetime', 'Updated At')" prop="updated_at" show-overflow-tooltip />
        <el-table-column :label="tr('taosuser.action', 'Action')" width="120">
          <template #default="{ row }">
            <el-button plain size="small" type="danger" icon="Delete" @click="revokeOAuthUser(row)"></el-button>
          </template>
        </el-table-column>
      </el-table>
    </template>
  </div>
</template>
<script setup lang="ts">
import UserForm from './components/userForm/index.vue';
import ImportInfo from './components/ImportForm/index.vue';
import { sendSQLReq } from '@/api/explorer';
import { useStore } from 'vuex';
import { useSorted } from '@vueuse/core';
import { oauthSyncUsers, oauthListExistingUsers, oauthRevoke } from '@/api/oauth';
import { promptSyncCredentials } from '@/utils/promptSyncCredentials';
import titleBar from './components/title-bar.vue';
import { formatDateInTimeZone } from 'taos-ui/utils/date';

const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY } = globalCustomProperties;

const { t } = useI18n();
const tr = (key: string, fallback: string) => {
  const val = t(key);
  return val === key ? fallback : val;
};
const store = useStore();
const isRoot = localStorage.getItem('username') === 'root';
const pageSize = ref(10);
const currentPage = ref(1);
const total = ref(10);
const dialog: Ref<boolean> = ref(false);
const editDialog: Ref<boolean> = ref(false);
const importDialog: Ref<boolean> = ref(false);
const loading: Ref<boolean> = ref(true);
const oauthEnabled = computed(() => store.state.app.isOAuthLogin);
const isOAuthSyncUsersSupported = computed(() => store.state.app.isOAuthSyncUsersSupported);
const syncing = ref(false);
const lastResult = ref<null | { imported: number; updated: number; skipped: number; message?: string }>(null);

let usersList: any[] = reactive([]);
const existingUsers = ref<any[]>([]);
const existLoading: Ref<boolean> = ref(false);
const editUser = ref('');
let currentUser = reactive<any>({});

function getCurrentUser() {
  store.dispatch('app/getUserInfo').then(res => {
    currentUser = res;
    console.log(currentUser);
  });
}

watch(
  () => oauthEnabled.value,
  enabled => {
    if (enabled) {
      getExistingOAuthUsers();
    } else {
      existingUsers.value = [];
    }
  },
  { immediate: true }
);

function closeDialog() {
  dialog.value = false;
  getUserData();
}

function closeEditDialog() {
  editDialog.value = false;
  getUserData();
}

function closeImportDialog() {
  importDialog.value = false;
}

function showDialog() {
  dialog.value = true;
}

function handlePageChange() {}

async function onSyncClick() {
  let credentials;
  try {
    credentials = await promptSyncCredentials({
      title: tr('taosuser.oauth2.syncConfirm', 'Enter SSO credentials to sync users'),
      passwordPrompt: tr('taosuser.oauth2.pleaseInputPassword', 'Please Input Password'),
      confirmButtonText: tr('common.sync', 'Sync'),
      cancelButtonText: tr('common.cancel', 'Cancel')
    });
  } catch (err) {
    console.log('Sync canceled', err);
    return;
  }
  // console.log('Sync credentials:', credentials);

  syncing.value = true;
  try {
    const res: any = await oauthSyncUsers(credentials);
    lastResult.value = {
      imported: res.imported ?? 0,
      updated: res.updated ?? 0,
      skipped: res.skipped ?? 0
    };
    ElMessage.success(tr('taosuser.oauth2.syncCompleted', 'User sync completed'));
  } catch (err: any) {
    console.warn('Sync user failed: ', err);
    const msg = err?.response?.data?.message || err?.message || tr('taosuser.oauth2.syncFailed', 'Sync failed');
    ElMessage.error(msg);
  } finally {
    syncing.value = false;
  }

  try {
    await getUserData();
  } catch (error) {
    console.error('Error fetching user data:', error);
  }

  try {
    await getExistingOAuthUsers();
  } catch (error) {
    console.error('Error fetching existing OAuth users:', error);
  }
}

function del(data: { name: string }) {
  ElMessageBox.confirm(t('isDel', [data.name]), t('warning'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(() => {
    sendSQLReq(`drop user \`${data.name}\``).then((res: { code: number }) => {
      if (res.code == 0) {
        ElMessage.success(t('delSucc'));
        getUserData();
      }
    });
  });
}

function edit(data: { name: string }) {
  ((editUser.value = data.name), (editDialog.value = true));
}

function changeState(data: { name: string; enable: number | string }) {
  let title = t('isDisable').replace('{isDisableName}', data.name);
  let state = 0;
  if (data.enable == 0) {
    title = t('isEnable').replace('{isDisableName}', data.name);
    state = 1;
  }
  ElMessageBox.confirm(title, {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  })
    .then(() => {
      sendSQLReq(`alter user \`${data.name}\` enable ${state}`).then(res => {
        if (res.code == 0) {
          ElMessage.success(t('operateSucc'));
          getUserData();
        }
      });
    })
    .catch(() => {});
}

async function getExistingOAuthUsers() {
  existLoading.value = true;
  try {
    const res: any = await oauthListExistingUsers();
    res.forEach((user: any) => {
      user.created_at = formatDateInTimeZone(user.created_at);
      user.updated_at = formatDateInTimeZone(user.updated_at);
    });
    existingUsers.value = res || [];
  } catch (error) {
    console.error('Error fetching existing OAuth users:', error);
    existingUsers.value = [];
  } finally {
    existLoading.value = false;
  }
}

async function revokeOAuthUser(user: { user_id: number; username: string }) {
  if (!user || !user.user_id) return;
  const name = user.username || '';
  try {
    await ElMessageBox.confirm(tr('taosuser.oauth2.revokeConfirm', `Revoke OAuth user ${name}?`), t('warning'), {
      confirmButtonText: t('confirm'),
      cancelButtonText: t('cancel'),
      type: 'warning'
    });
  } catch (err) {
    return;
  }

  try {
    await oauthRevoke(user.user_id);
    ElMessage.success(tr('taosuser.oauth2.revokeSuccess', 'OAuth user revoked'));
    await getExistingOAuthUsers();
  } catch (err: any) {
    console.error('Failed to revoke OAuth user:', err);
    const msg =
      err?.response?.data?.message || err?.message || tr('taosuser.oauth2.revokeFailed', 'Failed to revoke user');
    ElMessage.error(msg);
  }
}

async function getUserData() {
  try {
    loading.value = true;
    const res = await sendSQLReq(`select *
                                  from information_schema.ins_users;`);
    const permissionMap = res.data.map((data: { [x: string]: any }) => {
      return Object.fromEntries(
        res.column_meta.map((item: any[], index: string | number) => {
          return [item[0], data[index]];
        })
      );
    });

    const res1 = await sendSQLReq(`select *
                                   from information_schema.ins_user_privileges;`);
    const privilegeMap = res1.data.map((data: { [x: string]: any }) => {
      return Object.fromEntries(
        res.column_meta.map((item: any[], index: string | number) => {
          return [item[0], data[index]];
        })
      );
    });

    privilegeMap.forEach((data: { user_name: any; db_name: string | number; privilege: any }) => {
      const user = permissionMap.find((item: { name: any }) => item.name === data.user_name);

      if (user) {
        if (user.privilege === undefined) {
          user.privilege = {};
        }
        if (user.privilege[data.db_name] === undefined) {
          user.privilege[data.db_name] = [data.privilege];
        } else {
          user.privilege[data.db_name].push(data.privilege);
        }
      }
    });
    let rootUserIndex = permissionMap.findIndex((item: { name: string }) => item.name === 'root');
    const rooUser = permissionMap[rootUserIndex];
    rooUser.name = '*' + rooUser.name;
    permissionMap.unshift(rooUser);
    permissionMap.splice(++rootUserIndex, 1);
    const objSorted = useSorted(permissionMap, (a, b) => a.name.localeCompare(b.name));
    usersList = objSorted.value;

    loading.value = false;
  } catch (error) {
    loading.value = false;
    console.log(error);
    ElMessage.error('Get user error:', error);
  }
}

getUserData();
getCurrentUser();
</script>
<style lang="scss" scoped>
.line {
  width: 100%;
  height: 1px;
  margin: 20px 0;
  background-color: #ebeef5;
}
.settings-page {
  padding: 12px 0;
}
.settings-card {
  max-width: 100%;
}
.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
}
.desc {
  margin: 0 0 12px;
  color: var(--el-text-color-regular);
}
.actions {
  display: flex;
  gap: 8px;
}
.mb12 {
  margin-bottom: 12px;
}
.mt16 {
  margin-top: 16px;
}
.mt12 {
  margin-top: 12px;
}
.wd-small {
  width: 300px;
}
</style>
