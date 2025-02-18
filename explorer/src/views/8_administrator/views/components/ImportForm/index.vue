<template>
  <div>
    <el-form ref="ruleFormRef" :model="ruleForm" :rules="rules" label-width="auto" class="demo-ruleForm">
      <el-form-item :label="$t('taosuser.server')" prop="server">
        <el-input
          v-model.trim="ruleForm.server"
          autocomplete="off"
          placeholder="http://localhost:6041 / http://127.0.0.1:6041 "
        ></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.password')" prop="pwd">
        <el-input v-model.trim="ruleForm.pwd" autocomplete="new-password" clear :show-password="true"></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.items')" class="database-item" prop="selectedItems">
        <el-checkbox-group v-model="ruleForm.selectedItems" class="db-pri">
          <el-checkbox label="passwords" value="passwords">{{ $t('taosuser.userItem') }}</el-checkbox>
          <el-checkbox label="privileges" value="privileges">{{ $t('taosuser.privilegesItem') }}</el-checkbox>
          <el-checkbox label="whitelist" value="whitelist">{{ $t('taosuser.whitelistItem') }}</el-checkbox>
        </el-checkbox-group>
      </el-form-item>
      <el-alert v-if="showAlert" type="info" class="reason" @close="showAlert = false">
        <p>
          <span v-if="importReason?.success?.passwords"
            >{{ importReason?.success?.passwords }} {{ $t('taosuser.succ1') }}
          </span>
          <span v-if="importReason?.success?.passwords && importReason?.success?.privileges">、</span>
          <span v-if="importReason?.success?.privileges"
            >{{ importReason?.success?.privileges }} {{ $t('taosuser.succ2') }}</span
          >
          <span v-if="importReason?.success?.passwords || importReason?.success?.privileges">{{
            $i18n.locale.includes('en') ? ',' : '，'
          }}</span>
          <span
            v-if="importReason?.fails?.passwords?.length > 0 || importReason?.fails?.privileges?.length > 0"
            class="fail"
            >{{ $t('taosuser.fail1') }}</span
          >
        </p>
        <ul>
          <li v-for="(item, index) in importReason?.fails?.passwords" :key="'pwd' + index">
            {{ $t('taosuser.user') }} {{ item.user }} {{ $t('taosuser.fail2') }} {{ item.reason }}
          </li>
          <li v-for="(item, index) in importReason?.fails?.privileges" :key="'pri' + index">
            {{ $t('taosuser.user') }} {{ item.user }} {{ $t('taosuser.privilegesItem') }}(`{{ item.privilege }}`){{
              $t('taosuser.fail2')
            }}
            {{ item.reason }}
          </li>
        </ul>
        <br />
        <span v-if="importReason?.fails?.passwords?.length > 0 || importReason?.fails?.privileges?.length > 0">{{
          $t('taosuser.fail3')
        }}</span>
      </el-alert>
    </el-form>

    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button size="default" class="w100" @click="cancel">{{ $t('cancel') }}</el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button
          size="default"
          :disabled="confirmStatus"
          class="w100"
          type="primary"
          :loading="loading"
          @click="createUser(ruleFormRef)"
          >{{ $t('confirm') }}</el-button
        >
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { importTaosInfo } from '@/api/login';
import { FormInstance, FormRules } from 'element-plus';

const globalCustomProperties: any = inject('globalCustomProperties');
const { $error } = globalCustomProperties;

const { t } = useI18n();

const emit = defineEmits(['close', 'refresh']);

const ruleFormRef = ref<FormInstance>();

interface RuleForm {
  server: string;
  pwd: string;
  selectedItems: string[];
  passwords: boolean;
  privileges: boolean;
  whitelist: boolean;
}
const ruleForm = reactive<RuleForm>({
  server: '',
  pwd: '',
  selectedItems: ['passwords', 'privileges'],
  passwords: false,
  privileges: false,
  whitelist: false
});

const rules = reactive<FormRules<typeof ruleForm>>({
  server: [
    {
      required: true,
      message: t('taosuser.server') + t('requiredMessage')
    },
    {
      pattern: /^https?:\/\/((\d{1,3}\.){3}\d{1,3}|[a-zA-Z0-9.-]+)(:\d{1,5})$/,
      message: t('taosuser.formatError')
    }
  ],
  pwd: [
    {
      required: true,
      message: t('taosuser.password') + t('requiredMessage')
    }
  ],
  selectedItems: [
    {
      required: true,
      message: t('taosuser.items') + t('requiredMessage')
    }
  ]
});

const confirmStatus: Ref<boolean> = ref(false);
const loading: Ref<boolean> = ref(false);
const showAlert: Ref<boolean> = ref(false);
let importReason: Record<string, any> = reactive({
  // "success": {
  //   "passwords": 1,
  //   "privileges": 1,
  // },
  // "fails": {
  //   "passwords": [
  //     {
  //       "user": 'root',
  //       "reason": 'succ'
  //     }
  //   ],
  //   "privileges": [
  //     {
  //       "user": 'String',
  //       "privilege": 'String',
  //       "reason": 'Contact the TDengine customer success team to get the activation code'
  //     }
  //   ]
  // }
});

watch(
  () => ruleForm.selectedItems,
  items => {
    if (items) {
      // 当选中白名单时默认勾选 Passwords
      if (items.includes('whitelist') && !items.includes('passwords')) {
        ruleForm.selectedItems.push('passwords');
      }
    }
  },
  {
    deep: true
  }
);

function cancel() {
  emit('close');
}

function getServer() {
  // http://user:pwd@host:6041
  let url = '';
  const { server, pwd } = ruleForm;
  try {
    const parsed_url = new URL(server);
    const { protocol, host } = parsed_url;
    url = protocol + '//' + 'root:' + pwd + '@' + host;
  } catch (error) {
    console.log('error');
  }
  return url || server;
}
function getSelectItem(item: string) {
  return ruleForm.selectedItems.includes(item);
}
function createUser(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (valid) {
      loading.value = true;
      const params = {
        server: getServer(),
        passwords: getSelectItem('passwords'),
        privileges: getSelectItem('privileges'),
        whitelist: getSelectItem('whitelist')
      };
      try {
        const res = await importTaosInfo(params);
        if (res && Object.hasOwnProperty.call(res, 'code')) {
          loading.value = false;
          $error(res?.message);
          return;
        }
        importReason = res;
        showAlert.value = true;
        loading.value = false;
        emit('refresh');
        ElMessage.success(t('operateSucc'));
      } catch (error) {
        loading.value = false;
        console.log(error);
      }
    } else {
      return false;
    }
  });
}
</script>

<style lang="scss" scoped>
.db-label {
  display: inline-block;
  width: 240px;
  margin-right: 30px;
  text-align: left;
}

.db-pri {
  display: flex;
  flex-flow: column wrap;
  text-align: left;
}

.database-item {
  :deep(.el-form-item__content) {
    padding-top: 5px;
  }
}

.reason {
  font-weight: 500;
  text-align: left;

  :deep(.el-alert__description) {
    font-size: 14px;
  }

  ul {
    max-height: 400px;
    overflow-y: scroll;
    color: red;
  }
}
</style>
