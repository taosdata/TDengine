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
    <p class="flex-center">
      <el-radio-group v-model="model" size="default">
        <el-radio-button label="Wizard" value="Wizard"></el-radio-button>
        <el-radio-button label="SQL" value="SQL"></el-radio-button>
      </el-radio-group>
    </p>
    <SqlEditor v-show="model == 'SQL'" ref="sqlSrRef" v-model="sqlStr" :placeholder="sqlTip" height="150px">
    </SqlEditor>
    <template v-if="model == 'Wizard'">
      <el-form-item :label="$t('stream.streamName')" prop="stream_name">
        <el-input v-model="ruleForm.stream_name"> </el-input>
      </el-form-item>
      <h1 class="part-title">{{ $t('stream.output') }}</h1>
      <el-form-item :label="$t('stream.database')" required prop="target_db">
        <el-select v-model="ruleForm.target_db" class="w100" placeholder="" @change="ruleForm.target_stb = ''">
          <el-option v-for="item in dbList" :key="item.name" :value="item.name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.stable')" required prop="target_stb">
        <el-input v-model="ruleForm.target_stb" :disabled="!ruleForm.target_db"> </el-input>
      </el-form-item>
      <el-form-item :label="$t('stream.subtablePrefix')" prop="subtale">
        <el-input v-model="ruleForm.subtale" :disabled="!ruleForm.target_stb" placeholder=""> </el-input>
      </el-form-item>
      <h1 class="part-title">{{ $t('stream.source') }}</h1>
      <Subquery
        ref="subqueryRef"
        v-model:db-list="dbList"
        :level="ruleForm.source_type"
        :avg-fn="true"
        :window-clause="true"
        field-set
        :parttion="true"
        :info="ruleForm"
      >
        <template #db-bottom>
          <el-form-item :label="$t('type')" prop="source_type" required>
            <el-radio-group v-model="ruleForm.source_type">
              <el-radio-button :label="1" :value="1">{{ $t('stream.stableUpper') }}</el-radio-button>
              <el-radio-button :label="2" :value="2">{{ $t('stream.tableUpper') }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </Subquery>
      <h1 class="part-title">{{ $t('stream.execution') }}</h1>
      <el-form-item :label="$t('stream.trigger')">
        <el-select v-model="ruleForm.trigger" class="w100" placeholder="">
          <el-option v-for="item in triggerList" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="ruleForm.trigger == 'MAX_DELAY'" :label="$t('stream.maxDelayTime')">
        <el-input-number v-model="ruleForm.max_delay_time" :min="0"></el-input-number>
        <el-select v-model="ruleForm.max_delay_unit" style="margin-left: 20px" placeholder="">
          <el-option v-for="item in watermarkUnitList" :key="item.label" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.delay')">
        <template #label>
          <span>{{ $t('stream.delay') }}&nbsp;</span>
          <el-tooltip effect="light" :content="$t('stream.delaytip')" placement="top">
            <el-icon><InfoFilled /></el-icon>
          </el-tooltip>
        </template>
        <el-input-number v-model="ruleForm.watermark" :min="0" :max="watermarkMax"></el-input-number>
        <el-select
          v-model="ruleForm.watermark_unit"
          style="width: 180px; margin-left: 20px"
          placeholder=""
          @change="watermarkUnitChange"
        >
          <el-option v-for="item in watermarkUnitList" :key="item.label" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Ignore Expired">
        <template #label>
          <span>Ignore Expired</span>
        </template>
        <el-select v-model="ruleForm.ignore_expired" placeholder="" @change="changeIgnoreExpired">
          <el-option v-for="item in expiredList" :key="item.label" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="DELETE_MARK">
        <el-input-number v-model="ruleForm.deletemark" :min="0" :max="999999999999999"></el-input-number>
        <el-select v-model="ruleForm.deletemark_unit" style="width: 180px; margin-left: 20px" placeholder="">
          <el-option v-for="item in timeUnitList" :key="item.label" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="FILL_HISTORY">
        <el-select v-model="ruleForm.fill_history" placeholder="">
          <el-option v-for="item in expiredList" :key="item.label" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="IGNORE UPDATE">
        <el-select v-model="ruleForm.ignore_update" placeholder="">
          <el-option v-for="item in expiredList" :key="item.label" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
    </template>
    <p v-if="errorText" class="error-text">{{ errorText }}</p>
    <el-form-item v-if="model == 'Wizard'">
      <div class="flex-between flex1">
        <el-button style="width: 30%" :disabled="createBtn" type="primary" @click="handlecreateStream">
          {{ $t('create') }}
        </el-button>
        <el-button :disabled="previewBtn" @click="generateSql">{{ $t('sqlPreview') }}</el-button>
      </div>
    </el-form-item>
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
import Subquery from '@/views/6_topic/components/subquery.vue';
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

const validateTopicName = (_, val, callback) => {
  if (!val) {
    callback(new Error(t('stream.streamNameError')));
  } else if (props.streamList.some(item => item.stream_name === val)) {
    callback(new Error(t('stream.streamNameExist')));
  } else if (!validName(val)) {
    callback(new Error(t('formatWrong')));
  } else {
    callback();
  }
};

const sqlPrefix = 'CREATE STREAM ';
const subqueryRef = ref<InstanceType<typeof Subquery>>();
const ruleFormRef = ref<FormInstance>();
const ruleForm = reactive({
  db_name: '',
  target_db: '',
  target_stb: '',
  stbName: '',
  tbName: '',
  resultSet: [],
  source_type: 1,
  subtale: '',
  stream_name: '',
  parttionSet: 'tbname',
  window_type: 'INTERVAL',
  table_type: 'STABLE',
  tol_val: '',
  tol_unit: 'm',
  interval_val: 1,
  interval_offset: 0,
  state_column: '',
  interval_unit: 'm',
  offset_unit: 'm',
  sliding_val: 0,
  sliding_unit: 's',
  trigger: 'WINDOW_CLOSE',
  max_delay_time: '',
  max_delay_unit: 's',
  watermark: 0,
  watermark_unit: 's',
  ignore_expired: 1,
  deletemark_unit: 's'
});
const rules = reactive<FormRules>({
  stream_name: [
    {
      validator: validateTopicName,
      trigger: 'blur',
      required: true
    }
  ],
  target_stb: [
    {
      required: true,
      message: t('stream.stableUpperRequired')
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
const dbList = ref([]);

const watermarkMax = ref(15 * 60);
const expiredList = [
  {
    label: 1,
    value: 1
  },
  {
    label: 0,
    value: 0
  }
];
const watermarkUnitList = [
  {
    label: 'second',
    value: 's'
  },
  {
    label: 'minute',
    value: 'm'
  }
];
const timeUnitList = [
  {
    label: 'second',
    value: 's'
  },
  {
    label: 'minute',
    value: 'm'
  },
  {
    label: 'hour',
    value: 'h'
  },
  {
    label: 'week',
    value: 'w'
  },
  {
    label: 'day',
    value: 'd'
  }
];
const triggerList = [
  {
    label: 'WINDOW_CLOSE',
    value: 'WINDOW_CLOSE'
  },
  {
    label: 'AT_ONCE',
    value: 'AT_ONCE'
  },

  {
    label: 'MAX_DELAY',
    value: 'MAX_DELAY'
  }
];

const errorText = ref<string>('');
const requestIng = ref<boolean>(false);
const previewSqlStr = ref<string>('');
const dialog = ref<boolean>(false);
const sqlTip = 'CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery';

const previewBtn = computed(() => {
  if (model.value === 'Wizard') {
    if (!ruleForm.stream_name || !ruleForm.target_db || !ruleForm.db_name) return true;
    if (ruleForm.source_type === 1) {
      return !ruleForm.stbName;
    } else if (ruleForm.source_type === 2) {
      return !ruleForm.tbName;
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

function changeIgnoreExpired() {}
async function handlecreateStream() {
  errorText.value = '';
  if (requestIng.value) return;
  let sql = '';
  if (model.value === 'Wizard') {
    sql = await generateSql(false);
  } else {
    if (validStreamSql(sqlStr.value.trimStart())) {
      sql = sqlStr.value;
    } else {
      errorText.value = t('stream.validStreamSqlDesc');
      return;
    }
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
function watermarkUnitChange(val) {
  if (val === 's') {
    watermarkMax.value = 15 * 60;
  } else {
    if (ruleForm.watermark > 15) {
      ruleForm.watermark = 15;
    }
    watermarkMax.value = 15;
  }
}
function generateSql(show = true) {
  return new Promise((resolve, reject) => {
    ruleFormRef?.value?.validate(valid => {
      if (valid) {
        try {
          const subquery = subqueryRef?.value?.getResultSet() || '';
          let previewSql = sqlPrefix + '`' + ruleForm.stream_name + '`' + ' TRIGGER ' + ruleForm.trigger + ' ';

          if (ruleForm.trigger === 'MAX_DELAY') {
            previewSql += ruleForm.max_delay_time + ruleForm.max_delay_unit;
          }
          previewSql += `  IGNORE EXPIRED ${ruleForm.ignore_expired} `;
          if (ruleForm.watermark) {
            previewSql += ' WATERMARK ' + ruleForm.watermark + ruleForm.watermark_unit;
          }

          if (ruleForm.deletemark) {
            previewSql += ' DELETE_MARK ' + ruleForm.deletemark + ruleForm.deletemark_unit;
          }

          if (ruleForm.fill_history) {
            previewSql += `  FILL_HISTORY ${ruleForm.fill_history} `;
          }

          if (ruleForm.ignore_update) {
            previewSql += `  IGNORE UPDATE ${ruleForm.ignore_update} `;
          }
          previewSql += ' INTO `' + ruleForm.target_db.toLowerCase() + '`.`' + ruleForm.target_stb + '`';
          if (ruleForm.subtale) {
            previewSql += ` SUBTABLE(CONCAT('${ruleForm.subtale}',tbname))`;
          }
          previewSql += ' AS ' + subquery;
          if (ruleForm.parttionSet && ruleForm.parttionSet.length > 0) {
            previewSql += ' PARTITION BY ' + ruleForm.parttionSet;
          }
          if (ruleForm.window_type) {
            previewSql += ' ';
            const ts_col = ruleForm.resultSet.find(item => item.type === 'TIMESTAMP')?.field;
            switch (ruleForm.window_type) {
              case 'SESSION':
                previewSql += `SESSION(${ts_col},${ruleForm.tol_val}${ruleForm.tol_unit})`;
                break;
              case 'STATE':
                previewSql += `STATE_WINDOW(\`${ruleForm.state_column}\`)`;
                break;
              case 'INTERVAL':
                previewSql += `INTERVAL(${ruleForm.interval_val}${ruleForm.interval_unit}`;
                if (ruleForm.interval_offset != 0) {
                  previewSql += `,${ruleForm.interval_offset}${ruleForm.offset_unit}`;
                }
                previewSql += `)`;
                if (ruleForm.sliding_val) {
                  previewSql += ` SLIDING(${ruleForm.sliding_val}${ruleForm.sliding_unit})`;
                }
                break;
              default:
                break;
            }
          }
          previewSqlStr.value = previewSql;
          if (show) dialog.value = true;
          resolve(previewSql);
        } catch (error) {
          console.log(error);
          reject(error);
        }
      } else {
        reject();
      }
    });
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

  // &:deep(.CodeMirror) {
  //   height: 100px;
  // }
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
