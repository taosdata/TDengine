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
        </el-button
        >
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
      </el-button
      >
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
  </div>
</template>
<script setup lang="ts">
import UserForm from './components/userForm/index.vue';
import ImportInfo from './components/ImportForm/index.vue';
import {sendSQLReq} from '@/api/explorer';
import {useStore} from 'vuex';
import {useSorted} from '@vueuse/core';

const globalCustomProperties: any = inject('globalCustomProperties');
const {$IS_COMMUNITY} = globalCustomProperties;

const {t} = useI18n();
const store = useStore();
const isRoot = localStorage.getItem('username') === 'root';
const pageSize = ref(10);
const currentPage = ref(1);
const total = ref(10);
const dialog: Ref<boolean> = ref(false);
const editDialog: Ref<boolean> = ref(false);
const importDialog: Ref<boolean> = ref(false);
const loading: Ref<boolean> = ref(true);

let usersList: any[] = reactive([]);
const editUser = ref('');
let currentUser = reactive({});

function getCurrentUser() {
  store.dispatch('app/getUserInfo').then(res => {
    currentUser = res;
  });
}

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

function handlePageChange() {
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
  (editUser.value = data.name), (editDialog.value = true);
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
  }).then(() => {
    sendSQLReq(`alter user \`${data.name}\` enable ${state}`).then(res => {
      if (res.code == 0) {
        ElMessage.success(t('operateSucc'));
        getUserData();
      }
    });
  });
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
    ElMessage.error("Get user error:", error);
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
</style>
