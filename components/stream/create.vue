<template>
  <el-form
    ref="formRef"
    class="max-w-1000px w-full relative"
    hide-required-asterisk
    :rules="rules"
    label-width="150px"
    label-position="left"
    :model="info"
  >
    <el-steps class="mb-20px max-w-520px" simple :active="stepActive" finish-status="success">
      <el-step :title="t('common.output')" />
      <el-step :title="t('common.source')" />
      <el-step :title="t('stream.execution')" />
    </el-steps>
    <section>
      <SQLEditor v-show="model == 'SQL'" v-model="sqlStr" :placeholder="sqlTip"></SQLEditor>
      <div v-show="model == 'Wizard'">
        <div v-show="stepActive == 1">
          <el-form-item :label="t('common.name')" prop="stream_name">
            <el-input v-model="info.stream_name" maxlength="64"> </el-input>
          </el-form-item>
          <el-alert type="warning" class="mb-20px!" :title="t('stream.backslashTip')"></el-alert>
          <el-form-item :label="t('common.database')" prop="target_db">
            <el-select v-model="info.target_db" class="w-full" placeholder="" @change="info.target_stb = ''">
              <el-option v-for="item in dbList" :key="item.name" :value="item.name"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item :label="t('stb.stable')" prop="target_stb">
            <el-input v-model="info.target_stb" :disabled="!info.target_db"> </el-input>
          </el-form-item>
          <el-form-item :label="t('stream.subtablePrefix')" prop="subtale">
            <el-input v-model="info.subtale" :disabled="!info.target_stb" placeholder=""> </el-input>
          </el-form-item>
        </div>
        <Subquery
          v-show="stepActive == 2"
          ref="subQueryRef"
          v-model:db-list="dbList"
          v-model="info"
          :level="info.source_type"
          :avg-fn="true"
          :window-clause="true"
          field-set
          :parttion="true"
        >
          <template #db-bottom>
            <el-form-item :label="t('common.type')" prop="source_type" required>
              <el-radio-group v-model="info.source_type">
                <el-radio-button :value="1">{{ t('stb.stable') }}</el-radio-button>
                <el-radio-button :value="2">{{ t('stb.table') }}</el-radio-button>
              </el-radio-group>
            </el-form-item>
          </template>
        </Subquery>
        <div v-show="stepActive == 3">
          <el-form-item :label="t('stream.trigger')">
            <el-select v-model="info.trigger" class="w-full" placeholder="">
              <el-option v-for="item in triggerList" :key="item.value" v-bind="item"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item v-if="info.trigger == 'MAX_DELAY'" :label="t('stream.maxDelayTime')">
            <el-input-number v-model="info.max_delay_time" :min="0"></el-input-number>
            <el-select v-model="info.max_delay_unit" class="ml-20px" placeholder="">
              <el-option v-for="item in watermarkUnitList" :key="item.label" v-bind="item"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item :label="t('stream.delay')">
            <template #label>
              <span>{{ t('stream.delay') }}&nbsp;</span>
              <el-tooltip effect="light" :content="t('stream.delayTip')" placement="top">
                <el-icon :size="14">
                  <InfoFilled />
                </el-icon>
              </el-tooltip>
            </template>
            <el-input-number v-model="info.watermark" :min="0" :max="watermarkMax"></el-input-number>
            <el-select
              v-model="info.watermark_unit"
              class="ml-20px delay-select"
              placeholder=""
              @change="watermarkUnitChange"
            >
              <el-option v-for="item in watermarkUnitList" :key="item.label" v-bind="item"></el-option>
            </el-select>
          </el-form-item>
        </div>
      </div>
      <p v-if="errorText" class="errorText">{{ errorText }}</p>
      <div class="btn-wrapper">
        <el-button type="primary" :disabled="nextBtnDisabled" @click="stepActive++">{{ t('common.next') }}</el-button>
        <el-radio-group v-model="model">
          <el-radio-button :label="t('common.advanced')" value="Wizard"></el-radio-button>
          <el-radio-button label="SQL" value="SQL"></el-radio-button>
        </el-radio-group>
        <el-button v-if="model == 'Wizard'" :disabled="previewBtn" @click="generateSql()">Preview SQL</el-button>
        <el-button :disabled="createBtn" type="primary" @click="handlecreateStream">{{
          t('common.confirm')
        }}</el-button>
        <el-button :disabled="requestIng" @click="cancel">{{ t('common.cancel') }}</el-button>
      </div>
    </section>

    <el-dialog v-model="dialog" width="500px" append-to-body title="SQL">
      <CodeBlock :code="previewSql" />
      <section class="flex-end">
        <el-button type="primary" @click="dialog = false">{{ t('common.confirm') }}</el-button>
      </section>
    </el-dialog>
  </el-form>
</template>

<script lang="ts" setup>
import SQLEditor from '../SqlCodeEditor/index.vue';
import { createStream, streamList } from './api';
import { isStableExist } from '../api';
import { rmStrBackquote } from 'utils/tdengine';
import { ElMessage, FormInstance, FormRules } from 'element-plus';
import { t } from 'locales';
import Subquery from '../Subquery/index.vue';
import { DataItem } from 'components/SqlCondition/utils';
import CodeBlock from 'components/CodeBlock.vue';
import { useRouter } from 'hooks/useCurrentRouter';

const router = useRouter();
const sqlPrefix = 'CREATE STREAM ';
const watermarkMax = ref(900);
const stepActive = ref(1);
const model = ref('Wizard');
const sqlStr = ref('');
const rules: FormRules = {
  stream_name: [
    {
      required: true,
      message: t('common.requiredTemp', [t('common.name')]),
      trigger: 'blur'
    },
    {
      validator: (_: unknown, value: string, callback: AnyFunction) => {
        value = rmStrBackquote(value);
        if (streamList.value.some(item => item.stream_name === value)) {
          callback(new Error(t('common.existedTemp', [value])));
        } else {
          callback();
        }
      },
      trigger: 'blur'
    }
  ],
  target_db: [
    {
      required: true,
      message: t('common.requiredTemp', [t('db.target')]),
      trigger: 'blur'
    }
  ],
  target_stb: [
    {
      required: true,
      message: t('common.requiredTemp', [t('stb.target')]),
      trigger: 'blur'
    },
    {
      validator: (_: unknown, value: string, callback: AnyFunction) => {
        value = rmStrBackquote(value);
        isStableExist(value, info.target_db).then(res => {
          if (res) {
            callback(new Error(t('common.existedTemp', [value])));
          } else {
            callback();
          }
        });
      },
      trigger: 'blur'
    }
  ]
};
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
const triggerList = [
  {
    label: 'AT_ONCE',
    value: 'AT_ONCE'
  },
  {
    label: 'WINDOW_CLOSE',
    value: 'WINDOW_CLOSE'
  },
  {
    label: 'MAX_DELAY',
    value: 'MAX_DELAY'
  }
];
const dbList = ref<Recordable[]>([]);
const errorText = ref('');
const requestIng = ref(false);
const previewSql = ref('');
const dialog = ref(false);
const sqlTip = 'CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery';
const info = reactive({
  dbName: '',
  target_db: '',
  target_stb: '',
  stbName: '',
  tbName: '',
  resultSet: [] as Recordable[],
  conditionJson: [] as DataItem[],
  source_type: 1,
  subtale: '',
  stream_name: '',
  parttionSet: 'tbname',
  window_type: 'INTERVAL',
  table_type: 'STABLE',
  tol_val: 0,
  tol_unit: 'm',
  interval_val: 1,
  state_column: '',
  interval_unit: 'm',
  sliding_val: 0,
  sliding_unit: 's',
  trigger: 'WINDOW_CLOSE',
  max_delay_time: 0,
  max_delay_unit: 's',
  watermark: 0,
  watermark_unit: 's'
});
const formRef = shallowRef<FormInstance | null>(null);
const subQueryRef = shallowRef<InstanceType<typeof Subquery> | null>(null);
const previewBtn = computed(() => {
  if (model.value === 'Wizard') {
    if (!info.stream_name || !info.target_db || !info.dbName) return true;
    if (info.source_type === 1) {
      return !info.stbName;
    } else if (info.source_type === 2) {
      return !info.tbName;
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
const nextBtnDisabled = computed(() => {
  switch (stepActive.value) {
    case 1:
      return !info.stream_name || !info.target_db || !info.target_stb;
    case 2:
      return !info.dbName || (!info.stbName && !info.tbName);
    default:
      return true;
  }
});

async function handlecreateStream() {
  errorText.value = '';
  if (requestIng.value) return;
  let sql = '';
  if (model.value === 'Wizard') {
    sql = await generateSql(false);
  } else {
    handleSQLParams();
    if (errorText.value) return;
    sql = sqlStr.value;
  }
  requestIng.value = true;
  createStream(sql)
    .then(() => {
      try {
        formRef.value?.resetFields();
      } catch (error) {
        console.log(error);
      }
      sqlStr.value = '';
      ElMessage.success(t('msg.createSuccess'));
      cancel();
    })
    .catch(err => {
      if (err?.desc) {
        ElMessage.error(err?.desc);
        errorText.value = err?.desc;
      }
    })
    .finally(() => {
      requestIng.value = false;
    });
}

function cancel() {
  router.push('/stream');
}
function handleSQLParams() {
  const streamName = rmStrBackquote(sqlStr.value.match(/stream\s+(\S+)/i)?.[1] ?? '');
  if (!streamName) {
    errorText.value = t('common.requiredTemp', [t('common.name')]);
    return;
  }
  if (streamList.value.some(item => item.stream_name === streamName)) {
    errorText.value = t('common.existedTemp', [streamName]);
    return;
  }
  let databaseName = rmStrBackquote(sqlStr.value.match(/into\s+([^.\s]+)/i)?.[1] ?? '');
  handleDbIsExist(databaseName);
  if (errorText.value) return;
  databaseName = rmStrBackquote(sqlStr.value.match(/from\s+`*(\w+)/i)?.[1] ?? '');
  handleDbIsExist(databaseName);
}
function handleDbIsExist(database: string) {
  if (!database) {
    errorText.value = t('common.requiredTemp', [t('common.database')]);
  }
  if (!dbList.value.find(item => item.name === database)) {
    errorText.value = t('common.notExistedTemp', [database]);
  }
}
function watermarkUnitChange(val: string) {
  if (val === 's') {
    watermarkMax.value = 15 * 60;
  } else {
    if (info.watermark > 15) {
      info.watermark = 15;
    }
    watermarkMax.value = 15;
  }
}
function generateSql(show = true) {
  return new Promise<string>((resolve, reject) => {
    formRef.value?.validate(valid => {
      if (valid) {
        try {
          const subquery = subQueryRef.value?.generateSql() || '';
          let result = sqlPrefix + info.stream_name + ' TRIGGER ' + info.trigger + ' ';
          if (info.trigger === 'MAX_DELAY') {
            result += info.max_delay_time + info.max_delay_unit;
          }
          if (info.watermark) {
            result += ' WATERMARK ' + info.watermark + info.watermark_unit;
          }
          result += ' INTO `' + info.target_db + '`.`' + info.target_stb + '`';
          if (info.subtale) {
            result += ` SUBTABLE(CONCAT('${info.subtale}',tbname))`;
          }
          result += ' AS ' + subquery;
          if (info.parttionSet) {
            result += ' PARTITION BY ' + info.parttionSet;
          }
          if (info.window_type) {
            result += ' ';
            const ts_col = info.resultSet.find(item => item.type === 'TIMESTAMP')?.field;
            switch (info.window_type) {
              case 'SESSION':
                result += `SESSION(${ts_col},${info.tol_val}${info.tol_unit})`;
                break;
              case 'STATE':
                result += `STATE_WINDOW(\`${info.state_column}\`)`;
                break;
              case 'INTERVAL':
                result += `INTERVAL(${info.interval_val}${info.interval_unit})`;
                if (info.sliding_val) {
                  result += ` SLIDING(${info.sliding_val}${info.sliding_unit})`;
                }
                break;
              default:
                break;
            }
          }
          previewSql.value = result;
          if (show) dialog.value = true;
          resolve(result);
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

.delay-select {
  width: 100px;
  min-width: none;
}

.btn-wrapper {
  position: absolute;
  top: 10px;
  right: 10px;

  &:deep(.el-radio-group) {
    margin: 0 12px;
    font-size: inherit;
  }
}
</style>
<style>
.show-topic-sql .pre-code {
  padding: 5px;
  text-align: left;
  white-space: break-spaces;
  background-color: #f6f8fa;
}
</style>
