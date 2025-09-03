<template>
  <div v-loading="loading">
    <el-form ref="ruleFormRef" :model="ruleForm" :rules="rules" label-width="auto" class="demo-ruleForm">
      <el-form-item :label="$t('taosuser.username')" prop="user" required>
        <el-input v-model.trim="ruleForm.user" :disabled="isEdit" autocomplete="username"></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.password')" prop="pwd">
        <el-popover trigger="click" placement="right-end">
          <ol v-dompurify-html="$t(enableStrongPassword ? 'login.passwordTip' : 'login.passwordNotStrictTip')"
              style="padding-left: 10px; list-style: unset"></ol>
          <template #reference>
            <el-input v-model.trim="ruleForm.pwd" clear maxlength="255" :show-password="true" minlength="8" autocomplete="new-password" :placeholder="pwdtip"></el-input>
          </template>
        </el-popover>
      </el-form-item>
      <div class="line"></div>

      <!-- SYSINFO -->
      <el-form-item :label="$t('taosuser.sysinfo')">
        <el-popover trigger="hover" placement="right-end">
          <ol v-dompurify-html="$t('taosuser.sysinfoTip')" style="padding-left: 10px; list-style: unset"></ol>
          <template #reference>
            <el-switch v-model="ruleForm.sysinfo" :active-value="1" :inactive-value="0"/>
          </template>
        </el-popover>
      </el-form-item>

      <!-- CREATEDB -->
      <el-form-item :label="$t('taosuser.createdb')">
        <el-popover trigger="hover" placement="right-end">
          <ol v-dompurify-html="$t('taosuser.createdbTip')" style="padding-left: 10px; list-style: unset"></ol>
          <template #reference>
            <el-switch v-model="ruleForm.createdb" :active-value="1" :inactive-value="0"/>
          </template>
        </el-popover>
      </el-form-item>

      <!-- ALLOWED_HOST -->
      <el-form-item :label="$t('taosuser.allowed_host')">
        <el-popover trigger="hover" placement="right-end">
          <ol v-dompurify-html="$t('taosuser.allowed_hostTip')" style="padding-left: 10px; list-style: unset"></ol>
          <template #reference>
            <el-input-tag v-model="ruleForm.allowed_host"></el-input-tag>
          </template>
        </el-popover>
      </el-form-item>

      <!-- DATABASE privileges -->
      <el-form-item v-if="databaseList.length > 0" :label="$t('taosuser.database')" class="database-item">
        <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!$IS_COMMUNITY">
          <template #content>
            <span v-dompurify-html="$t('communityTip')"></span>
          </template>
          <ul>
            <li v-for="item in databaseList" :key="item">
              <label class="db-label">{{ item }}</label>
              <el-checkbox-group v-model="selectedDatabasePrivileges[item]" class="db-pri">
                <el-checkbox :disabled="$IS_COMMUNITY" label="Read" value="Read">{{ $t('read') }}</el-checkbox>
                <el-checkbox :disabled="$IS_COMMUNITY" label="Write" value="Write">{{ $t('write') }}</el-checkbox>
              </el-checkbox-group>
            </li>
          </ul>
        </el-tooltip>
      </el-form-item>

      <!-- topic privileges -->
      <el-form-item v-if="topicList.length > 0" :label="$t('taosuser.subscription')" class="database-item">
        <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!$IS_COMMUNITY">
          <template #content>
            <span v-dompurify-html="$t('communityTip')"></span>
          </template>
          <ul>
            <li v-for="item in topicList" :key="item">
              <label class="db-label">{{ item }}</label>
              <el-checkbox-group v-model="selectedTopicPrivileges[item]" class="db-pri">
                <el-checkbox :disabled="$IS_COMMUNITY" label="Subscribe" value="Subscribe">{{
                    $t('subscribe')
                  }}
                </el-checkbox>
              </el-checkbox-group>
            </li>
          </ul>
        </el-tooltip>
      </el-form-item>
    </el-form>

    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button size="default" class="w100" @click="cancel">{{ $t('cancel') }}</el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button size="default" :disabled="confirmStatus" class="w100" type="primary" @click="submit(ruleFormRef)">{{
            $t('confirm')
          }}
        </el-button>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import {sendSQLReq} from '@/api/explorer';
import {getDatabaseVariables} from '@/api/database';
import {FormInstance, FormRules} from 'element-plus';
import {validPassword, validPasswordNotStrict} from '@/utils/validate';

const globalCustomProperties: any = inject('globalCustomProperties');
const {$IS_COMMUNITY, $error} = globalCustomProperties;

const {t} = useI18n();

const emit = defineEmits(['close']);

const props = defineProps({
  user: {
    type: String,
    default: ''
  },
  status: {
    type: Boolean,
    default: false
  }
});

const checkPassword = async (_: any, value: string, callback: (arg0: Error | undefined) => void) => {
  callback(validatePasswordLocal(value) ? undefined : new Error(t('login.passwordError')));
};

const enableStrongPassword = ref(false);
const validatePasswordLocal = (value: string) => {
  if (value.trim().length === 0) {
    return true
  }
  if (enableStrongPassword.value) {
    return validPassword(value);
  } else {
    return validPasswordNotStrict(value);
  }
};

const ruleFormRef = ref<FormInstance>();
const isEdit = computed(() => props.user !== '');
const pwdtip = computed(() => {
  return isEdit.value ? t('taosuser.passwordEditTip') : '';
});

interface RuleForm {
  user: string;
  pwd: string;
  sysinfo: number;
  createdb: number;
  allowed_host: string[];
}

const ruleForm = reactive<RuleForm>({
  user: '',
  pwd: '',
  sysinfo: 1,
  createdb: 0,
  allowed_host: [],
});

const rules = reactive<FormRules<typeof ruleForm>>({
  user: [
    {
      required: true,
      message: t('taosuser.username') + t('requiredMessage')
    }
  ],
  pwd: [
    {
      required: !isEdit.value,
      message: t('taosuser.password') + t('requiredMessage')
    },
    {validator: checkPassword, trigger: 'blur'}
  ]
});
const databaseList = reactive<string[]>([]);
const topicList = reactive<string[]>([]);
const prevDatabasePrivileges: Record<string, any> = reactive({});
let prevTopicPrivileges: Record<string, any> = reactive({});
const selectedDatabasePrivileges: Record<string, any> = reactive({});
const selectedTopicPrivileges: Record<string, any> = reactive({});
const loading: Ref<boolean> = ref(true);
const confirmStatus: Ref<boolean> = ref(false);
const ruleFormOld = ref<RuleForm>({
  user: '',
  pwd: '',
  sysinfo: 1,
  createdb: 0,
  allowed_host: []
});

watch(
  () => props.status,
  async val => {
    if (val) {
      loading.value = true;
      ruleForm.user = props.user;
      await getDatabaseList();
      await getTopicList();
      if (isEdit.value) {
        await getUserPrivileges();
        await getUserTopics();
      } else {
        ruleFormOld.value.sysinfo = 1;
        ruleForm.sysinfo = 1;
        ruleFormOld.value.createdb = 0;
        ruleForm.createdb = 0;
        ruleFormOld.value.allowed_host = [];
        ruleForm.allowed_host = [];
        loading.value = false;
      }
    } else {
      cancel()
    }
  },
  {
    deep: true,
    immediate: true
  }
);

async function getDatabaseList() {
  try {
    const res = await sendSQLReq(`show databases;`);
    const databaseArr = res.data.map((data: { [x: string]: any }) => {
      return Object.fromEntries(
        res.column_meta.map((item: any[], index: string | number) => {
          return [item[0], data[index]];
        })
      );
    });
    databaseList.splice(0, databaseList.length);
    databaseArr.forEach((item: { name: string }) => {
      if (['performance_schema', 'information_schema'].indexOf(item.name) < 0) {
        databaseList.push(item.name);
        if (isEdit.value) {
          selectedDatabasePrivileges[item.name] = [];
        } else {
          const privilege = $IS_COMMUNITY ? ['Read', 'Write'] : ['Read'];
          selectedDatabasePrivileges[item.name] = privilege;
        }
      }
    });
  } catch (error) {
    console.log(error);
  }
}

async function getTopicList() {
  try {
    const res = await sendSQLReq(`show topics;`);
    const topicArr = res.data.map((data: { [x: string]: any }) => {
      return Object.fromEntries(
        res.column_meta.map((item: any[], index: string | number) => {
          return [item[0], data[index]];
        })
      );
    });
    topicList.splice(0, topicList.length);
    topicArr.forEach((item: { topic_name: string }) => {
      topicList.push(item.topic_name);
      selectedTopicPrivileges[item.topic_name] = [];
    });
  } catch (error) {
    console.log(error);
  }
}

async function getUserPrivileges() {
  try {
    const user = await sendSQLReq(
      `select *
       from information_schema.ins_users
       where name = '${ruleForm.user}';`
    );
    if (user.data && user.data.length > 0) {
      const userRow = user.data[0];
      const meta = user.column_meta;
      const fieldIndex: Record<string, number> = {};
      meta.forEach((item: string[], index: number) => {
        fieldIndex[item[0]] = index;
      });
      // set sysinfo
      ruleFormOld.value.sysinfo = userRow[fieldIndex['sysinfo']];
      ruleForm.sysinfo = userRow[fieldIndex['sysinfo']];
      // set createdb
      ruleFormOld.value.createdb = userRow[fieldIndex['createdb']];
      ruleForm.createdb = userRow[fieldIndex['createdb']];
      // set allowed_host
      const allowedHost = userRow[fieldIndex['allowed_host']] || '';
      const allowedHostArr = allowedHost.split(',') || [];
      ruleFormOld.value.allowed_host = allowedHostArr;
      ruleForm.allowed_host = allowedHostArr;
    }

    const res = await sendSQLReq(
      `select *
       from information_schema.ins_user_privileges
       where user_name = '${ruleForm.user}'
         and privilege <> 'subscribe';`
    );
    res.data.map((data: string[]) => {
      if (selectedDatabasePrivileges[data[2]] === undefined) {
        const name = data[2];
        const pri = data[1].slice(0, 1).toUpperCase() + data[1].slice(1);

        selectedDatabasePrivileges[name] = [pri];
        prevDatabasePrivileges[name] = [pri];
      } else {
        const name = data[2];
        const pri = data[1].slice(0, 1).toUpperCase() + data[1].slice(1);
        selectedDatabasePrivileges[name].push(pri);
        selectedDatabasePrivileges[data[2]] = selectedDatabasePrivileges[name];
        prevDatabasePrivileges[data[2]] = selectedDatabasePrivileges[name];
      }
    });
  } catch (error) {
    console.log(error);
  }
}

async function getUserTopics() {
  try {
    const res = await sendSQLReq(
      `select *
       from information_schema.ins_user_privileges
       where user_name = '${ruleForm.user}'
         and privilege = 'subscribe';`
    );
    loading.value = false;
    res.data.map((data: (string | number)[]) => {
      selectedTopicPrivileges[data[2]] = ['Subscribe'];
      prevTopicPrivileges = selectedTopicPrivileges;
    });
  } catch (error) {
    console.log(error);
  }
}

function cancel() {
  emit('close');
  databaseList.splice(0, databaseList.length);
  Object.keys(selectedDatabasePrivileges).forEach(key => {
    delete selectedDatabasePrivileges[key]
  });
  Object.keys(selectedTopicPrivileges).forEach(key => {
    delete selectedTopicPrivileges[key]
  });
  Object.keys(prevDatabasePrivileges).forEach(key => {
    delete prevDatabasePrivileges[key]
  });
  Object.keys(prevTopicPrivileges).forEach(key => {
    delete prevTopicPrivileges[key]
  });
  topicList.splice(0, topicList.length);
}

async function grantPrivilege(privileges: string, dbName: string, userName: string) {
  return await sendSQLReq(`GRANT ${privileges} ON \`${dbName}\`.*  to \`${userName}\``)
  .then((res: any) => {
    return Promise.resolve(res);
  })
  .catch((err: any) => {
    return Promise.reject(err);
  });
}

async function grantTopic(topicName: string, userName: string) {
  return await sendSQLReq(`GRANT subscribe ON \`${topicName}\` to \`${userName}\``)
  .then((res: any) => {
    return Promise.resolve(res);
  })
  .catch((err: any) => {
    return Promise.reject(err);
  });
}

async function alterUser() {
  return await sendSQLReq(`alter USER \`${props.user}\` PASS '${ruleForm.pwd}';`)
  .then((res: any) => {
    return Promise.resolve(res);
  })
  .catch((err: any) => {
    return Promise.reject(err);
  });
}

async function cancelPrivilege(privilege: string, dbName: string) {
  return await sendSQLReq(`REVOKE ${privilege} ON \`${dbName}\`.* FROM \`${props.user}\`;`)
  .then((res: any) => {
    return Promise.resolve(res);
  })
  .catch((err: any) => {
    return Promise.reject(err);
  });
}

async function cancelTopic(topicName: string) {
  return await sendSQLReq(`REVOKE subscribe ON \`${topicName}\` FROM \`${props.user}\`;`)
  .then((res: any) => {
    return Promise.resolve(res);
  })
  .catch((err: any) => {
    return Promise.reject(err);
  });
}

function createUser(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(valid => {
    if (valid) {
      try {
        const allowedHosts = ruleForm.allowed_host && ruleForm.allowed_host.length > 0 ? Array.from(new Set(ruleForm.allowed_host.map(h => `'${h.trim()}'`).filter(Boolean))) : [];
        const hostStr = allowedHosts.length > 0 ? `HOST ${allowedHosts.join(',')}` : '';
        return sendSQLReq(
          `CREATE USER \`${ruleForm.user}\` PASS '${ruleForm.pwd}' SYSINFO ${ruleForm.sysinfo} CREATEDB ${ruleForm.createdb} ${hostStr};`
        )
        .then(() => {
          for (const key in selectedDatabasePrivileges) {
            if (selectedDatabasePrivileges[key].length > 0) {
              const privileges = selectedDatabasePrivileges[key];
              privileges.forEach(async (item: string) => {
                await grantPrivilege(item, key, ruleForm.user);
              });
            }
          }
          for (const key in selectedTopicPrivileges) {
            if (selectedTopicPrivileges[key].length > 0) {
              const privileges = selectedTopicPrivileges[key];
              privileges.forEach(async () => {
                await grantTopic(key, ruleForm.user);
              });
            }
          }
          ElMessage.success(t('taosuser.createNewUserSucTip'));
          cancel();
        })
        .catch((err: { desc: any }) => {
          err && err.desc && $error(err.desc);
          return Promise.reject(err);
        });
      } catch (error) {
        console.log(error);
      }
    } else {
      return false;
    }
  });
}

function editUser(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (valid) {
      try {
        if (ruleForm.pwd) {
          await alterUser();
        }
        // alter sysinfo
        if (ruleFormOld.value.sysinfo !== ruleForm.sysinfo) {
          await sendSQLReq(`ALTER USER \`${props.user}\` SYSINFO ${ruleForm.sysinfo};`);
        }
        // alter createdb
        if (ruleFormOld.value.createdb !== ruleForm.createdb) {
          await sendSQLReq(`ALTER USER \`${props.user}\` CREATEDB ${ruleForm.createdb};`);
        }
        // alter allowed_host
        const oldHost = new Set(ruleFormOld.value.allowed_host);
        const newHost = new Set(ruleForm.allowed_host);
        // drop hosts
        for (const host of oldHost) {
          if (!newHost.has(host)) {
            await sendSQLReq(`ALTER USER \`${props.user}\` DROP HOST '${host}';`);
          }
        }
        // add hosts
        for (const host of newHost) {
          if (!oldHost.has(host)) {
            await sendSQLReq(`ALTER USER \`${props.user}\` ADD HOST '${host}';`);
          }
        }

        for (const key in prevDatabasePrivileges) {
          const privileges = prevDatabasePrivileges[key];

          if (selectedDatabasePrivileges[key] === undefined) {
            for (const item of privileges) {
              await cancelPrivilege(item, key);
            }
          } else {
            for (const item of privileges) {
              if (selectedDatabasePrivileges[key].indexOf(item) === -1) {
                await cancelPrivilege(item, key);
              }
            }
          }
        }
        for (const key in selectedDatabasePrivileges) {
          if (selectedDatabasePrivileges[key].length > 0) {
            const privileges = selectedDatabasePrivileges[key];
            for (const item of privileges) {
              await grantPrivilege(item, key, props.user);
            }
          }
        }

        for (const key in prevTopicPrivileges) {
          if (selectedTopicPrivileges[key] === undefined) {
            await cancelTopic(key);
          } else {
            if (selectedTopicPrivileges[key].indexOf('Subscribe') === -1) {
              await cancelTopic(key);
            }
          }
        }

        for (const key in selectedTopicPrivileges) {
          if (selectedTopicPrivileges[key].length > 0) {
            await grantTopic(key, ruleForm.user);
          }
        }
        ElMessage.success(t('operateSucc'));
        cancel();
      } catch (error: any) {
        console.log(error);
        if (error && error.desc) {
          $error(error.desc);
        }
      }
    } else {
      return false;
    }
  });
}

function submit(formEl: FormInstance | undefined) {
  if (isEdit.value) {
    editUser(formEl);
  } else {
    createUser(formEl);
  }
}

onMounted(async () => {
  const result = await getDatabaseVariables('enableStrongPassword');
  enableStrongPassword.value = (result === true || result === 'true' || result === '1');
});

</script>

<style lang="scss" scoped>
.db-label {
  display: inline-block;
  width: 240px;
  margin-right: 30px;
  text-align: left;
}

.db-pri {
  display: inline-block;
  width: 215px;
  text-align: left;
}

.database-item {
  li {
    padding-left: 2px;
    text-align: left;
  }

  :deep(.el-form-item__content) {
    padding-top: 5px;
  }
}
</style>
