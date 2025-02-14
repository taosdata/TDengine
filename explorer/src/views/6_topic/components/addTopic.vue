<template>
  <el-form
    ref="ruleFormRef"
    class="add-topic"
    hide-required-asterisk
    :rules="rules"
    style="text-align: left"
    size="default"
    label-width="120px"
    label-position="left"
    :model="info"
  >
    <p class="flex-center">
      <el-radio-group v-model="model" size="default">
        <el-radio-button label="Wizard" value="Wizard"></el-radio-button>
        <el-radio-button label="SQL" value="SQL"></el-radio-button>
      </el-radio-group>
    </p>
    <!-- <SQLEditor
      v-show="model == 'SQL'"
      ref="sqlStr"
      v-model="sqlStr"
      :placeholder="sqlTip"
    ></SQLEditor> -->
    <SqlEditor v-show="model == 'SQL'" v-model="sqlStr" :placeholder="sqlTip" height="150px"> </SqlEditor>
    <template v-if="model == 'Wizard'">
      <el-form-item :label="$t('topic.topicName')" prop="topic_name">
        <el-input v-model="info.topic_name" maxlength="32"> </el-input>
      </el-form-item>
      <Subquery
        ref="subqueryRef"
        :db-list="dbList"
        :field-set="fieldSet"
        :level="subqueryLevel"
        :info="info"
        @db-change="onDBChange"
      >
        <template #db-bottom>
          <el-form-item :label="$t('type')" prop="topic_type" required>
            <el-radio-group v-model="info.topic_type">
              <el-radio-button label="DATABASE" value="DATABASE">{{ $t('stream.databaseUpper') }}</el-radio-button>
              <el-radio-button label="STABLE" value="STABLE">{{ $t('stream.stableUpper') }}</el-radio-button>
              <el-radio-button label="SUBQUERY" value="SUBQUERY">{{ $t('stream.subqueryUpper') }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="info.topic_type != 'SUBQUERY'" :label="$t('topic.withMeta')" prop="with_meta">
            <el-switch v-model="info.with_meta"></el-switch>
          </el-form-item>
          <el-form-item v-if="info.topic_type == 'SUBQUERY'" :label="$t('stream.tableType')" prop="table_type" required>
            <el-radio-group v-model="info.table_type">
              <el-radio-button label="STABLE" value="STABLE">{{ $t('stream.stableUpper') }}</el-radio-button>
              <el-radio-button label="TABLE" value="TABLE">{{ $t('stream.tableUpper') }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </Subquery>
    </template>
    <p v-if="errorText" class="error-text">{{ errorText }}</p>
    <el-form-item v-if="model == 'Wizard'">
      <div class="flex-between flex1">
        <el-button
          style="width: 30%"
          :loading="requestIng"
          :disabled="createBtn"
          type="primary"
          @click="handleCreateTopic"
          >{{ $t('create') }}</el-button
        >
        <el-button :disabled="previewBtn" @click="generateSql(true)">{{ $t('sqlPreview') }}</el-button>
      </div>
    </el-form-item>
    <div v-else class="flex-center">
      <el-button
        size="default"
        style="width: 30%; margin-top: 20px"
        :disabled="createBtn"
        type="primary"
        @click="handleCreateTopic"
        >{{ $t('create') }}</el-button
      >
    </div>
    <el-dialog v-model="dialog" custom-class="show-topic-sql" width="500px" title="SQL" :close-on-click-modal="false">
      <pre :key="previewSql" v-highlight>
        <code class="language-sql">{{ previewSql }}</code>
      </pre>
      <section class="flex-end">
        <el-button type="primary" size="default" @click="dialog = false">{{ $t('confirm') }}</el-button>
      </section>
    </el-dialog>
  </el-form>
</template>

<script setup lang="ts">
import Subquery from './subquery.vue';
import SqlEditor from 'taos-ui/components/SqlCodeEditor/index.vue';
import { createTopic } from '@/api/topic';
import { validTopicSql, validName } from '@/utils/validate';
import { FormInstance, FormRules } from 'element-plus';
// import SQLEditor from "./sqlEditor.vue";
const { t } = useI18n();
const emit = defineEmits(['close']);
const ruleFormRef = ref<FormInstance>();
const subqueryRef = ref<InstanceType<typeof Subquery>>();
const props = withDefaults(
  defineProps<{
    topicList: any[];
  }>(),
  {
    topicList: () => []
  }
);

const validateTopicName = (_, val, callback) => {
  if (!val) {
    callback(new Error(t('topic.topicNameError')));
  } else if (props.topicList.some(item => item.topic_name === val)) {
    callback(new Error(t('topic.topicNameExist')));
  } else if (!validName(val)) {
    callback(new Error(t('formatWrong')));
  } else {
    callback();
  }
};
const sqlTip = 'CREATE TOPIC [IF NOT EXISTS] topic_name AS {subquery | DATABASE db_name | STABLE stb_name }';
const sqlPrefix = 'CREATE TOPIC ';
const rules = reactive<FormRules>({
  topic_name: [
    {
      validator: validateTopicName,
      trigger: 'blur',
      required: true
    }
  ],
  stbName: [
    {
      required: true,
      message: t('stream.stableUpperRequired')
    }
  ],
  tbName: [
    {
      required: true,
      message: t('stream.tableUpperRequired')
    }
  ]
});
const sqlStr = ref<string>('');
const model = ref('Wizard');
const info = reactive({
  db_name: '',
  stbName: '',
  tbName: '',
  resultSet: [],
  topic_type: 'STABLE',
  topic_name: '',
  table_type: 'STABLE',
  with_meta: false
});
const dbList = ref([]);
const errorText = ref('');
const requestIng = ref<boolean>(false);
const dialog = ref<boolean>(false);
const previewSql = ref('');

const previewBtn = computed(() => {
  if (model.value === 'Wizard') {
    if (!info.topic_name) return true;
    if (!info.db_name) return true;
    if (info.topic_type === 'STABLE') {
      return !info.stbName;
    } else if (info.topic_type === 'SUBQUERY') {
      return info.table_type == 'STABLE' ? !info.stbName : !info.tbName;
    } else {
      return false;
    }
  } else {
    return true;
  }
});
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

async function handleCreateTopic() {
  errorText.value = '';
  if (requestIng.value) return;
  let params = {};
  if (model.value === 'Wizard') {
    await generateSql(false);
    params = previewSql.value;
  } else {
    const sqlobj = handleSQLParams();
    if (validTopicSql(sqlobj.topic_sql.trimStart())) {
      params = sqlobj.topic_sql;
    } else {
      errorText.value = t('topic.validTopicSqlDesc');
      return;
    }
  }
  requestIng.value = true;
  createTopic(params)
    .then(() => {
      ruleFormRef?.value?.resetFields();
      info.stbName = '';
      info.tbName = '';
      sqlStr.value = '';
      ElMessage.success(t('addSucc'));
      emit('close');
    })
    .catch(err => (errorText.value = err?.desc))
    .finally(() => {
      requestIng.value = false;
    });
}
function handleSQLParams() {
  let database: string | undefined = '';
  let topic_type = '';
  const topic_name = sqlStr.value.match(/topic\s+(\w+)/i)?.[1];
  if (/database/i.test(sqlStr.value)) {
    database = sqlStr.value.match(/database\s+`*(\w+)/i)?.[1];
    topic_type = 'DATABASE';
  } else if (/stable/i.test(sqlStr.value)) {
    database = sqlStr.value.match(/stable\s+`*(\w+)/i)?.[1];
    topic_type = 'STABLE';
  } else {
    database = sqlStr.value.match(/from\s+`*(\w+)/i)?.[1];
    topic_type = 'SUBQUERY';
  }
  const database_id = database ? dbList.value.find(item => item.name === database)?.name : '';

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
    ruleFormRef?.value?.validate(valid => {
      if (valid) {
        const dbname = info.db_name;
        if (info.topic_type == 'DATABASE') {
          previewSql.value =
            sqlPrefix +
            '`' +
            info.topic_name +
            '`' +
            (info.with_meta ? ' WITH META' : '') +
            ' AS DATABASE `' +
            dbname +
            '`';
        } else if (info.topic_type == 'STABLE') {
          previewSql.value =
            sqlPrefix +
            '`' +
            info.topic_name +
            '`' +
            (info.with_meta ? ' WITH META' : '') +
            ` AS STABLE \`${dbname}\`.\`${info.stbName}\``;
        } else {
          const subquery = subqueryRef?.value?.getResultSet() || '';
          previewSql.value = sqlPrefix + '`' + info.topic_name + '`' + ' AS ' + subquery;
        }
        if (show) dialog.value = true;
        resolve(previewSql.value);
      } else {
        reject();
      }
    });
  });
}

function onDBChange() {
  info.stbName = '';
  info.tbName = '';
}
</script>

<style scoped lang="scss">
.add-topic {
  .flex-center {
    margin-bottom: 20px;
  }

  .vue-codemirror {
    height: 100px;
    margin-bottom: 20px;
  }

  // &:deep(.CodeMirror) {
  //   height: 100px;

  //   .CodeMirror-placeholder {
  //     color: #c0c4cc;
  //   }
  // }
}

.pre-code {
  display: inline-flex;
  padding: 10px 5px 20px;
  text-align: left;
  background-color: #f6f8fa;
}

.language-sql {
  word-break: break-all;
  word-wrap: break-word;
  white-space: normal;
}
</style>
