<template>
  <div v-loading="loading">
    <el-form ref="ruleFormRef" :model="ruleForm" :rules="rules" label-width="auto" class="demo-ruleForm">
      <el-form-item :label="$t('taosuser.username')" prop="user" required>
        <el-input v-model.trim="ruleForm.user" :disabled="isEdit" autocomplete="username"></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.password')" prop="pwd">
        <el-popover trigger="click" placement="right-end">
          <ol
            v-dompurify-html="$t(enableStrongPassword ? 'login.passwordTip' : 'login.passwordNotStrictTip')"
            style="padding-left: 10px; list-style: unset"
          ></ol>
          <template #reference>
            <el-input
              v-model.trim="ruleForm.pwd"
              clear
              maxlength="255"
              :show-password="true"
              minlength="8"
              autocomplete="new-password"
              :placeholder="pwdtip"
            ></el-input>
          </template>
        </el-popover>
      </el-form-item>
      <div class="line"></div>

      <!-- SYSINFO -->
      <el-form-item :label="$t('taosuser.sysinfo')">
        <el-popover trigger="hover" placement="right-end">
          <ol v-dompurify-html="$t('taosuser.sysinfoTip')" style="padding-left: 10px; list-style: unset"></ol>
          <template #reference>
            <el-switch v-model="ruleForm.sysinfo" :active-value="1" :inactive-value="0" />
          </template>
        </el-popover>
      </el-form-item>

      <!-- CREATEDB -->
      <el-form-item :label="$t('taosuser.createdb')">
        <el-popover trigger="hover" placement="right-end">
          <ol v-dompurify-html="$t('taosuser.createdbTip')" style="padding-left: 10px; list-style: unset"></ol>
          <template #reference>
            <el-switch v-model="ruleForm.createdb" :active-value="1" :inactive-value="0" />
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
                <el-checkbox :disabled="$IS_COMMUNITY" label="Read" :value="READ_PRIV">{{ $t('read') }}</el-checkbox>
                <el-checkbox :disabled="$IS_COMMUNITY" label="Write" :value="WRITE_PRIV">{{ $t('write') }}</el-checkbox>
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
            <li v-for="{ topic_name, db_name } in topicList" :key="topic_name">
              <label class="db-label">{{ topic_name }}</label>
              <div class="db-pri">
                <el-checkbox
                  :disabled="$IS_COMMUNITY"
                  :model-value="selectedTopicPrivileges[topic_name]?.length > 0"
                  @change="(val: string | number | boolean) => onTopicChange(topic_name, db_name, !!val)"
                >
                  {{ $t('subscribe') }}
                </el-checkbox>
              </div>
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
        <el-button size="default" :disabled="confirmStatus" class="w100" type="primary" @click="submit(ruleFormRef)"
          >{{ $t('confirm') }}
        </el-button>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { getDatabaseVariables } from '@/api/database';
import { FormInstance, FormRules } from 'element-plus';
import { validPassword, validPasswordNotStrict } from '@/utils/validate';
import { compareVersion, getTDVersion } from '@/utils';

const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY, $error } = globalCustomProperties;

const { t } = useI18n();

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

const checkPassword = (_: any, value: string, callback: (arg0: Error | undefined) => void) => {
  callback(validatePasswordLocal(value) ? undefined : new Error(t('login.passwordError')));
};

const enableStrongPassword = ref(false);
const validatePasswordLocal = (value: string) => {
  if (value.trim().length === 0) {
    return true;
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
  allowed_host: []
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
    { validator: checkPassword, trigger: 'blur' }
  ]
});
const databaseList = reactive<string[]>([]);
const topicList = reactive<{ db_name: string; topic_name: string }[]>([]);
const prevDatabasePrivileges: Record<string, any> = reactive({});
const prevTopicPrivileges: Record<string, any> = reactive({});
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

const tdVersion = getTDVersion();
const verLessThan3400 = compareVersion(tdVersion, "<3.4.0.0");

const READ_PRIV = verLessThan3400 ? 'Read' : 'SELECT';
const WRITE_PRIV = verLessThan3400 ? 'Write' : 'INSERT';
const SUBSCRIBE_PRIV = verLessThan3400 ? 'Subscribe' : 'SUBSCRIBE';

function onTopicChange(topicName: string, dbName: string, checked: boolean) {
  if (checked) {
    selectedTopicPrivileges[topicName] = [{ priv_type: SUBSCRIBE_PRIV, database: dbName }];
  } else {
    selectedTopicPrivileges[topicName] = [];
  }
}

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
      cancel();
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
          const privilege = $IS_COMMUNITY ? [READ_PRIV, WRITE_PRIV] : [READ_PRIV];
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
    const res = await sendSQLReq(`select db_name, topic_name from information_schema.ins_topics;`);
    const topicArr = res.data.map((data: { [x: string]: any }) => {
      return Object.fromEntries(
        res.column_meta.map((item: any[], index: string | number) => {
          return [item[0], data[index]];
        })
      );
    });
    topicList.splice(0, topicList.length);
    topicArr.forEach((item: { db_name: string; topic_name: string }) => {
      topicList.push({ db_name: item.db_name, topic_name: item.topic_name });
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

    const privFilter = verLessThan3400 ? "privilege <> 'subscribe'" : "priv_type <> 'SUBSCRIBE'" ;

    const res = await sendSQLReq(
      `select *
       from information_schema.ins_user_privileges
       where user_name = '${ruleForm.user}' and ${privFilter};`
    );
    res.data.map((data: string[]) => {
      const dbName = verLessThan3400 ? data[2] : data[3];
      if (dbName) {
        const pri = verLessThan3400 ? data[1].slice(0, 1).toUpperCase() + data[1].slice(1) : data[1].toUpperCase();
        if (selectedDatabasePrivileges[dbName] === undefined) {
          selectedDatabasePrivileges[dbName] = [pri];
          prevDatabasePrivileges[dbName] = [pri];
        } else {
          selectedDatabasePrivileges[dbName].push(pri);
          prevDatabasePrivileges[dbName] = [...selectedDatabasePrivileges[dbName]];
        }
      }
    });
  } catch (error) {
    console.log(error);
  }
}

async function getUserTopics() {
  try {
    const privFilter = verLessThan3400 ? "privilege = 'subscribe'" : "priv_type='SUBSCRIBE'";
    const res = await sendSQLReq(`select *
       from information_schema.ins_user_privileges
       where user_name = '${ruleForm.user}' and ${privFilter}`);
    loading.value = false;
    res.data.map((data: (string)[]) => {
      // For old version (<3.4.0): db_name stores topic name, table_name is empty
      // For new version (>=3.4.0): db_name stores database name, obj_name stores topic name
      const topicName = verLessThan3400 ? data[2] : data[4];
      const dbName = verLessThan3400 ? '' : data[3];
      // Always set the privilege since getTopicList() already initialized all topics to []
      selectedTopicPrivileges[topicName] = [{ priv_type: SUBSCRIBE_PRIV, database: dbName }];
      prevTopicPrivileges[topicName] = [{ priv_type: SUBSCRIBE_PRIV, database: dbName }];
    });
  } catch (error) {
    console.log(error);
  }
}

function cancel() {
  emit('close');
  databaseList.splice(0, databaseList.length);
  Object.keys(selectedDatabasePrivileges).forEach(key => {
    delete selectedDatabasePrivileges[key];
  });
  Object.keys(selectedTopicPrivileges).forEach(key => {
    delete selectedTopicPrivileges[key];
  });
  Object.keys(prevDatabasePrivileges).forEach(key => {
    delete prevDatabasePrivileges[key];
  });
  Object.keys(prevTopicPrivileges).forEach(key => {
    delete prevTopicPrivileges[key];
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

async function grantTopic(topicName: string, userName: string, databaseName: string) {
  const sql = verLessThan3400 ? `GRANT subscribe ON \`${topicName}\` to \`${userName}\`` : `GRANT subscribe ON topic \`${databaseName}\`.\`${topicName}\` to \`${userName}\``;
  return await sendSQLReq(sql)
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

async function cancelTopic(topicName: string, databaseName: string) {
  const sql = verLessThan3400 ? `REVOKE subscribe ON \`${topicName}\` FROM \`${props.user}\`;` : `REVOKE subscribe ON topic \`${databaseName}\`.\`${topicName}\` FROM \`${props.user}\`;`;
  return await sendSQLReq(sql)
    .then((res: any) => {
      return Promise.resolve(res);
    })
    .catch((err: any) => {
      return Promise.reject(err);
    });
}

function createUser(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate((valid: boolean) => {
    if (valid) {
      try {
        const allowedHosts =
          ruleForm.allowed_host && ruleForm.allowed_host.length > 0
            ? Array.from(new Set(ruleForm.allowed_host.map(h => `'${h.trim()}'`).filter(Boolean)))
            : [];
        const hostStr = allowedHosts.length > 0 ? `HOST ${allowedHosts.join(',')}` : '';
        return sendSQLReq(
          `CREATE USER \`${ruleForm.user}\` PASS '${ruleForm.pwd}' SYSINFO ${ruleForm.sysinfo} CREATEDB ${ruleForm.createdb} ${hostStr};`
        )
          .then(async () => {
            for (const key in selectedDatabasePrivileges) {
              if (selectedDatabasePrivileges[key].length > 0) {
                const privileges = selectedDatabasePrivileges[key];
                for (const item of privileges) {
                  await grantPrivilege(item, key, ruleForm.user);
                }
              }
            }
            for (const topicName in selectedTopicPrivileges) {
              if (selectedTopicPrivileges[topicName].length > 0) {
                await grantTopic(topicName, ruleForm.user, selectedTopicPrivileges[topicName][0].database);
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
      return;
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

        for (const topicName in prevTopicPrivileges) {
          if (selectedTopicPrivileges[topicName] === undefined) {
            await cancelTopic(topicName, prevTopicPrivileges[topicName][0].database);
          } else {
            if (selectedTopicPrivileges[topicName].length === 0) {
              await cancelTopic(topicName, prevTopicPrivileges[topicName][0].database);
            }
          }
        }

        for (const topicName in selectedTopicPrivileges) {
          if (selectedTopicPrivileges[topicName].length > 0) {
            await grantTopic(topicName, ruleForm.user, selectedTopicPrivileges[topicName][0].database);
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
      return;
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
  enableStrongPassword.value = result === true || result === 'true' || result === '1';
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
