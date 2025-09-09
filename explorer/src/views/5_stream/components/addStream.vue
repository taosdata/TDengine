<template>
  <el-form
    ref="ruleFormRef"
    class="add-topic"
    hide-required-asterisk
    :rules="rules"
    style="text-align: left"
    size="default"
    label-width="150px"
    label-position="left"
    :model="ruleForm"
  >
    <SqlEditor ref="sqlSrRef" v-model="sqlStr" :placeholder="sqlTip" height="150px">
    </SqlEditor> 
    <p v-if="errorText" class="error-text">{{ errorText }}</p>
    <div v-else class="flexCenter">
      <el-button
        size="default"
        style="width: 30%; margin-top: 15px"
        :loading="requestIng"
        :disabled="createBtn"
        type="primary"
        @click="handlecreateStream"
        >{{ $t('create') }}</el-button
      >
    </div>
    <el-dialog
      v-model="dialog"
      custom-class="show-topic-sql"
      width="500px"
      append-to-body
      title="SQL"
      :close-on-click-modal="false"
    >
      <pre :key="previewSqlStr" v-highlight>
        <code class="language-sql">{{ previewSqlStr }}</code>
      </pre>
      <section class="flex-end">
        <el-button type="primary" size="default" @click="dialog = false">{{ $t('confirm') }}</el-button>
      </section>
    </el-dialog>
  </el-form>
</template>

<script setup lang="ts">
import SqlEditor from 'taos-ui/components/SqlCodeEditor/index.vue';
import { createStream } from '@/api/stream';
import { validStreamSql, validName } from '@/utils/validate';
import type { FormInstance, FormRules } from 'element-plus';

const { t } = useI18n();
const props = defineProps({
  streamList: {
    type: Array,
    default: () => []
  }
});

const emit = defineEmits(['close']);

const validateTopicName = (_:any, val: any, callback: any) => {
  if (!val) {
    callback(new Error(t('stream.streamNameError')));
  } else if (props.streamList.some((item: any) => item.stream_name === val)) {
    callback(new Error(t('stream.streamNameExist')));
  } else if (!validName(val)) {
    callback(new Error(t('formatWrong')));
  } else {
    callback();
  }
};

const ruleFormRef = ref<FormInstance>();
const ruleForm = reactive({
  db_name: '',
  resultSet: [],
  stream_name: '', 
});
const rules = reactive<FormRules>({
  stream_name: [
    {
      validator: validateTopicName,
      trigger: 'blur',
      required: true
    }
  ]
});

const sqlStr = ref<string>('');

const errorText = ref<string>('');
const requestIng = ref<boolean>(false);
const previewSqlStr = ref<string>('');
const dialog = ref<boolean>(false);
const sqlTip = 'CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name options [INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]';

const createBtn = computed(() => {
  return requestIng.value || !sqlStr.value;
});

async function handlecreateStream() {
  errorText.value = '';
  if (requestIng.value) return;
  let sql = '';
  if (validStreamSql(sqlStr.value.trimStart())) {
    sql = sqlStr.value;
  } else {
    errorText.value = t('stream.validStreamSqlDesc');
    return;
  }
  requestIng.value = true;
  createStream(sql)
    .then(() => {
      ruleFormRef?.value?.resetFields();
      sqlStr.value = '';
      ElMessage.success(t('addSucc'));
      emit('close');
    })
    .catch(err => (errorText.value = err?.desc))
    .finally(() => {
      requestIng.value = false;
    });
}
</script>

<style scoped lang="scss">
.source-content {
  padding: 10px;
}

.part-title {
  font-size: 18px;
  font-weight: bold;
  line-height: 36px;
  color: #4d6992;
  text-align: center;
}

.add-topic {
  .flex-center {
    margin-bottom: 20px;
  }

  .vue-codemirror {
    height: 100px;
    margin-bottom: 20px;
  }
}

:deep(.el-input-number__increase),
:deep(.el-input-number__decrease) {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 30px;
}

.language-sql {
  word-break: break-all;
  word-wrap: break-word;
  white-space: normal;
}

.pre-code {
  display: inline-flex;
  padding: 5px;
  padding: 10px 5px 20px;
  text-align: left;

  /* white-space: break-spaces; */
  white-space: pre-wrap;
  background-color: #f6f8fa;
}
</style>
