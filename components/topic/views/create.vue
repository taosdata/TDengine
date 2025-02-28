<template>
  <el-form
    ref="formRef"
    class="max-w-1000px w-full"
    hide-required-asterisk
    :rules="rules"
    style="text-align: left"
    label-width="120px"
    label-position="left"
    :model="info"
  >
    <SQLEditor v-show="model == 'SQL'" v-model="sqlStr" :placeholder="sqlTip"></SQLEditor>
    <template v-if="model == 'Wizard'">
      <el-form-item :label="t('topic.name')" prop="topic_name">
        <el-input v-model="info.topic_name" :maxlength="64"> </el-input>
      </el-form-item>
      <el-alert type="warning" :title="t('topic.backslashTip')"></el-alert>
      <Subquery ref="subqueryRef" v-model:db-list="dbList" v-model="info" :field-set="fieldSet" :level="subqueryLevel">
        <template #db-bottom>
          <el-form-item :label="t('common.type')" prop="topic_type" required>
            <el-radio-group v-model="info.topic_type">
              <el-radio-button value="DATABASE">{{ t('common.database') }}</el-radio-button>
              <el-radio-button value="STABLE">{{ t('stb.stable') }}</el-radio-button>
              <el-radio-button value="SUBQUERY">{{ t('stb.subquery') }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="info.topic_type == 'SUBQUERY'" :label="t('stb.tableType')" prop="table_type" required>
            <el-radio-group v-model="info.table_type">
              <el-radio-button value="STABLE">{{ t('stb.stable') }}</el-radio-button>
              <el-radio-button value="TABLE">{{ t('stb.table') }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </Subquery>
    </template>
    <p v-if="errorText" v-dompurify-html="errorText" class="errorText"></p>
    <WalRentionPeriodTip v-if="walRentionPeriodShow" :db-name="info.dbName"></WalRentionPeriodTip>
    <el-form-item v-if="model == 'Wizard'">
      <div class="flexBetween">
        <el-button :loading="requestIng" :disabled="createBtn" type="primary" @click="handleCreateTopic">{{
          t('common.create')
        }}</el-button>
        <el-button :disabled="previewBtn" @click="generateSql()">{{ t('common.preview') }} SQL</el-button>
      </div>
    </el-form-item>
    <div v-else class="flexCenter">
      <el-button class="w50" :disabled="createBtn" type="primary" @click="handleCreateTopic">{{
        t('common.create')
      }}</el-button>
    </div>
    <el-dialog v-model="dialog" width="500px" append-to-body title="SQL">
      <CodeBlock :code="previewSql" language="sql" />
      <section class="flex-end">
        <el-button type="primary" @click="dialog = false">{{ t('common.confirm') }}</el-button>
      </section>
    </el-dialog>
  </el-form>
</template>

<script lang="ts" setup>
import { ElMessage, FormInstance, FormRules } from 'element-plus';
import SQLEditor from '../../SqlCodeEditor/index.vue';
import { createTopic } from '../api';
import Subquery from '../../Subquery/index.vue';
import WalRentionPeriodTip from '../components/walRentionPeriodTip.vue';
import { rmStrBackquote } from 'utils/tdengine';
import { t } from 'locales';
import CodeBlock from 'components/CodeBlock.vue';

const props = withDefaults(
  defineProps<{
    topicList: Recordable[];
  }>(),
  {
    topicList: () => []
  }
);
const rules: FormRules = {
  topic_name: [
    {
      required: true,
      message: t('common.requiredTemp', [t('common.name')]),
      trigger: 'blur'
    },
    {
      validator: (_: unknown, value: string, callback: AnyFunction) => {
        value = rmStrBackquote(value);
        if (props.topicList.some(item => item.topicName === value)) {
          callback(new Error(t('common.existedTemp', [value])));
        } else {
          callback();
        }
      },
      trigger: 'blur'
    }
  ]
};
const sqlPrefix = 'CREATE TOPIC ';
const sqlStr = ref('');
const model = ref('Wizard');
const info = reactive({
  dbName: '',
  stbName: '',
  tbName: '',
  resultSet: [],
  topic_type: 'STABLE',
  topic_name: '',
  table_type: 'STABLE',
  conditionJson: []
});
const dbList = ref<Recordable[]>([]);
const errorText = ref('');
const walRentionPeriodShow = ref(false);
const requestIng = ref(false);
const previewSql = ref('');
const dialog = ref(false);
const sqlTip = 'CREATE TOPIC [IF NOT EXISTS] topic_name AS {subquery | DATABASE db_name | STABLE stb_name }';

const previewBtn = computed(() => {
  if (model.value !== 'Wizard') return true;
  if (!info.topic_name) return true;
  if (!info.dbName) return true;
  if (info.topic_type === 'STABLE') {
    return !info.stbName;
  } else if (info.topic_type === 'SUBQUERY') {
    return info.table_type === 'STABLE' ? !info.stbName : !info.tbName;
  } else {
    return false;
  }
});
const formRef = shallowRef<FormInstance | null>(null);
const subqueryRef = shallowRef<InstanceType<typeof Subquery> | null>(null);

const createBtn = computed(() => {
  return requestIng.value || (model.value === 'Wizard' && previewBtn.value) || (model.value === 'SQL' && !sqlStr.value);
});
const subqueryLevel = computed(() => {
  return {
    DATABASE: 0,
    STABLE: 1,
    SUBQUERY: {
      STABLE: 1,
      TABLE: 2
    }[info.table_type]
  }[info.topic_type];
});
const fieldSet = computed(() => {
  return model.value === 'Wizard' && info.topic_type === 'SUBQUERY';
});
const emits = defineEmits(['close']);

async function handleCreateTopic() {
  errorText.value = '';
  walRentionPeriodShow.value = false;
  if (requestIng.value) return;
  let params: Recordable = {};
  if (model.value === 'Wizard') {
    await generateSql(false);
    if (!previewSql.value) return;
    params = {
      database_id: dbList.value.find(item => item.name === info.dbName)?.databaseId,
      topic_sql: previewSql.value,
      topic_type: info.topic_type,
      topic_name: info.topic_name.startsWith('`') ? info.topic_name.replace(/`/g, '') : info.topic_name.toLowerCase(),
      db_name: info.dbName
    };
  } else {
    params = handleSQLParams()!;
    if (errorText.value) return;
  }
  requestIng.value = true;
  createTopic(params)
    .then(() => {
      try {
        formRef.value?.resetFields();
      } catch (error) {
        console.log(error);
      }
      info.stbName = '';
      info.tbName = '';
      sqlStr.value = '';
      ElMessage.success(t('msg.createSuccess'));
      emits('close');
    })
    .catch(err => {
      if (err?.code == '908') {
        errorText.value = '';
        walRentionPeriodShow.value = true;
      } else {
        errorText.value = err?.desc;
      }
    })
    .finally(() => {
      requestIng.value = false;
    });
}
function handleSQLParams(): Recordable | undefined {
  let database;
  let topic_type = '';
  const topic_name = rmStrBackquote(sqlStr.value.match(/topic\s+(\S+)/i)?.[1]);
  if (!topic_name) {
    errorText.value = t('common.requiredTemp', [t('topic.name')]);
    return;
  }
  if (props.topicList.find(item => item.topicName === topic_name)) {
    errorText.value = t('common.existedTemp', [topic_name]);
    return;
  }
  if (/database/i.test(sqlStr.value)) {
    database = sqlStr.value.match(/database\s+([^.\s]+)/i)?.[1];
    topic_type = 'DATABASE';
  } else if (/stable/i.test(sqlStr.value)) {
    database = sqlStr.value.match(/stable\s+([^.\s]+)/i)?.[1];
    topic_type = 'STABLE';
  } else {
    database = sqlStr.value.match(/from\s+([^.\s]+)/i)?.[1];
    topic_type = 'SUBQUERY';
  }
  database = rmStrBackquote(database);
  const database_id = database ? dbList.value.find(item => item.name === database)?.databaseId : '';
  if (!database_id) {
    errorText.value = t('common.notExistedTemp', [database]);
    return;
  }
  return {
    database_id,
    topic_type,
    topic_name,
    topic_sql: sqlStr.value,
    db_name: database
  };
}
function generateSql(show = true) {
  return new Promise((resolve, reject) => {
    formRef.value?.validate(valid => {
      if (valid) {
        const dbname = info.dbName;
        const prefixSql = sqlPrefix + info.topic_name;
        if (info.topic_type == 'DATABASE') {
          previewSql.value = prefixSql + ' WITH META AS DATABASE `' + dbname + '`';
        } else if (info.topic_type == 'STABLE') {
          previewSql.value = prefixSql + ` WITH META AS STABLE \`${dbname}\`.\`${info.stbName}\``;
        } else {
          const subquery = subqueryRef.value?.generateSql() || '';
          previewSql.value = prefixSql + ' AS ' + subquery;
        }
        if (show) dialog.value = true;
        resolve(previewSql.value);
      } else {
        reject();
      }
    });
  }).catch(err => {
    console.log(err);
  });
}
</script>

<style scoped lang="scss"></style>
