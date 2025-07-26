<template>
  <div>
    <div class="flex-end">
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!$IS_COMMUNITY">
        <template #content>
          <span v-dompurify-html="$t('communityTip')"></span>
        </template>
        <el-button
          class="big-button"
          plain
          type="primary"
          :disabled="localUser !== 'root' || $IS_COMMUNITY"
          size="default"
          icon="Plus"
          @click="addShareTopicUser"
          >{{ $t('topic.addShareTopicUser') }}</el-button
        >
      </el-tooltip>
    </div>
    <el-table style="margin-top: 20px" size="default" :data="subscriptionList">
      <el-table-column :label="$t('topic.user_name')" prop="user_name"></el-table-column>
      <el-table-column :label="$t('taosuser.action')" width="150">
        <template #default="scope">
          <el-switch
            :model-value="scope.row.enable == 1"
            :disabled="scope.row.super === 1 || !currentUser.super"
            style="--el-switch-on-color: #13ce66; --el-switch-off-color: #dcdfe6"
            @change="changeState(scope.row)"
          >
          </el-switch>
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
    >
    </el-pagination>
    <el-dialog
      v-model="dialog"
      align="center"
      :title="$t('topic.add_new_user')"
      width="400px"
      :destroy-on-close="true"
      :close-on-click-modal="false"
    >
      <el-form ref="ruleFormRef" :model="ruleForm" label-width="120px" class="demo-ruleForm" :rules="rules">
        <el-form-item :label="$t('topic.user_name')" prop="user_name">
          <el-select v-model="ruleForm.user_name" style="width: 100%">
            <el-option v-for="item in userList" :key="item.name" :label="item.name" :value="item.name"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button
            type="primary"
            style="width: 100%; height: 32px; padding: 4px 20px"
            @click="submotForm(ruleFormRef)"
            >{{ $t('add') }}</el-button
          >
        </el-form-item>
      </el-form>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { FormInstance, FormRules } from 'element-plus';
import { useStore } from 'vuex';
const { $IS_COMMUNITY } = inject('globalCustomProperties') as GlobalCustomProperties;

const props = defineProps({
  topicId: {
    type: String,
    default: ''
  }
});
const { t } = useI18n();
const store = useStore();

const ruleFormRef = ref<FormInstance>();
const localUser = localStorage.getItem('username');
const subscriptionList = ref([]);
const dialog = ref<boolean>(false);
const userList = ref<any[]>([]);
const currentPage = ref(1);
const pageSize = ref(10);
const total = ref(0);
const ruleForm = reactive({
  user_name: '',
  expire_time: ''
});
const rules = reactive<FormRules>({
  user_name: [
    {
      required: true,
      message: t('topic.user_name_required')
    }
  ]
});
let currentUser = reactive<Recordable>({});

watch(
  () => props.topicId,
  () => {
    getUserData();
  },
  {
    deep: true
  }
);

function getCurrentUser() {
  store.dispatch('app/getUserInfo').then(res => {
    currentUser = Object.assign(currentUser, res);
  });
}

async function addUser() {
  try {
    if (props.topicId) {
      await sendSQLReq(`grant subscribe on \`${props.topicId}\`.* to ${ruleForm.user_name};`).then(res => {
        if (res.rows) {
          ElMessage.success(t('operateSucc'));
          getUserData();
        }
      });
    } else {
      ElMessage({
        type: 'error',
        message: t('topic.select_topic_tip')
      });
    }
    dialog.value = false;
  } catch (error) {
    console.log(error);
  }
}
function submotForm(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(valid => {
    if (valid) {
      addUser();
    }
  });
}

function handlePageChange() {}
function changeState(data) {
  let title = t('isDisable').replace('{isDisableName}', data.user_name);
  // let state = 0;
  if (data.enable == 0) {
    title = t('isEnable').replace('{isDisableName}', data.user_name);
    // state = 1;
  }
  ElMessageBox.confirm(title, t('warning'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(() => {
    sendSQLReq(`revoke subscribe on \`${props.topicId}\`.* from ${data.user_name}`).then(res => {
      if (res.code == 0) {
        ElMessage.success(t('operateSucc'));
        getUserData();
      }
    });
  });
}
async function getUserData() {
  try {
    const usersRes = await sendSQLReq(`select * from information_schema.ins_users;`);
    const usersMap = usersRes.data.map(data => {
      return Object.fromEntries(
        usersRes.column_meta.map((item, index) => {
          return [item[0], data[index]];
        })
      );
    });
    const res = await sendSQLReq(
      `select user_name from information_schema.ins_user_privileges where privilege in ('all', 'subscribe') and db_name in ('${props.topicId}', 'all');`
    );
    const privilegeMap = res.data.map(data => {
      return Object.fromEntries(
        res.column_meta.map((item, index) => {
          return [item[0], data[index]];
        })
      );
    });
    const permissionMap = privilegeMap.map(item => {
      const user = usersMap.find(data => data.name === item.user_name);
      item.enable = 1;
      item.super = user.super;
      return item;
    });
    const noSubscriptionList = usersMap.filter(item => {
      return privilegeMap.every(data => data.user_name != item.name);
    });
    let rootUserIndex = permissionMap.findIndex(item => item.user_name === 'root');
    const rooUser = permissionMap[rootUserIndex];
    rooUser.user_name = '*' + rooUser.user_name;
    permissionMap.unshift(rooUser);
    permissionMap.splice(++rootUserIndex, 1);
    subscriptionList.value = permissionMap;
    userList.value = noSubscriptionList;
  } catch (error) {
    console.log(error);
  }
}
function addShareTopicUser() {
  dialog.value = true;
  ruleForm.user_name = '';
  getUserData();
}
getUserData();
getCurrentUser();
</script>

<style scoped="scss">
.el-picker-panel__footer .el-button--text.el-picker-panel__link-btn {
  display: none;
}
</style>
