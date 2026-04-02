<!-- eslint-disable vue/no-mutating-props -->
<template>
  <div>
    <el-table tooltip-effect="dark" style="width: 100%" :data="modelValue" size="small">
      <el-table-column width="40">
        <template #default="scope">
          <el-checkbox v-model="scope.row.checked"></el-checkbox>
        </template>
      </el-table-column>
      <el-table-column :label="$t('name')" show-overflow-tooltip prop="name" min-width="120"> </el-table-column>
      <el-table-column prop="type" :label="$t('type')" show-overflow-tooltip width="120"> </el-table-column>
      <el-table-column prop="result" :label="$t('topic.resultSet')" width="120">
        <template #default="{ row, $index }">
          <el-button :disabled="!row.fnList" icon="Setting" size="small" @click="result(row, $index)"></el-button>
        </template>
      </el-table-column>
      <el-table-column prop="condition" :label="$t('topic.conditionSet')" width="120">
        <template #default="{ row, $index }">
          <el-button
            :disabled="!row.conditionList.length"
            icon="Setting"
            size="small"
            @click="result(row, $index, 1)"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog v-model="dialog" append-to-body width="400px" center :title="title" :close-on-click-modal="false">
      <component :is="comp" v-bind="dialogParams" :field="field"></component>
      <template #footer>
        <section>
          <el-button @click="dialog = false">{{ $t('cancel') }}</el-button>
          <el-button type="primary" @click="confirm">{{ $t('confirm') }}</el-button>
        </section>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import Result from './result.vue';
import Condition from './condition.vue';
import { deepClone } from '@/utils';
import { getMatrixStructReq } from '@/api/tables';
import { getStableStructReq } from '@/api/stables';
import {
  NumericFn,
  StringFn,
  ConversionFn,
  DatetimeFN,
  AggregationFn,
  SelectorFn,
  SeriesSpecificFn,
  SystemFn,
  TDengineStringType,
  TDengineNumberType,
  CompareOperator,
  JsonOperator,
  GeneralOperator,
  RegularOperator
} from '@/const';
import { isArray } from '@/utils/validate';
const fnMap = {
  NUMBER: NumericFn,
  STRING: StringFn,
  CONVERSION: ConversionFn,
  DATETIME: DatetimeFN,
  AVGFN: AggregationFn,
  SELECTION: SelectorFn,
  SERIES: SeriesSpecificFn,
  SYSTEM: SystemFn
};
const fnMapName = new Map([
  ['NUMBER', 'NumericFn'],
  ['STRING', 'StringFn'],
  ['CONVERSION', 'ConversionFn'],
  ['DATETIME', 'DatetimeFN'],
  ['AVGFN', 'AggregationFn'],
  ['SELECTION', 'SelectorFn'],
  ['SERIES', 'SeriesSpecificFn'],
  ['SYSTEM', 'SystemFn']
]);
const getGeneralFn = type => {
  return GeneralOperator.filter(item => !type.includes(item.label)).map(item => item.label);
};
const conditionMap = {
  TIMESTAMP: CompareOperator.concat(getGeneralFn(['TIMESTAMP'])),
  NUMBER: CompareOperator.concat(getGeneralFn(['NUMBER'])),
  STRING: RegularOperator.concat(getGeneralFn(['STRING'])),
  JSON: JsonOperator,
  BOOL: CompareOperator.concat(getGeneralFn(['NOT BETWEEN AND', 'BETWEEN AND']))
};

const { t } = useI18n();
const props = defineProps({
  params: {
    type: Object,
    default: () => ({})
  },
  modelValue: {
    type: Array,
    default: () => []
  },
  avgFn: {
    type: Boolean,
    default: false
  }
});
const emit = defineEmits(['update:columns', 'update:tags', 'update:modelValue']);

const options = ref([]);
// const fnList = ref([]);
// const tags = ref([]);
const dialog = ref(false);
const dialogType = ref(0);
const dialogParams = reactive({});
const currentRowIndex = ref(-1);
const field = ref('');

const comp = computed(() => {
  return {
    0: Result,
    1: Condition
  }[dialogType.value];
});
const title = computed(() => {
  return {
    0: t('topic.resultSet'),
    1: t('topic.conditionSet')
  }[dialogType.value];
});

watch(
  () => props.params,
  () => {
    getData();
  },
  {
    deep: true,
    immediate: true
  }
);

function getData() {
  if (!props.params.selected_db || (!props.params.selected_tb && !props.params.stableName))
    return emit('update:modelValue', []);
  const dataFn = props.params.stableName ? getStableStructReq : getMatrixStructReq;
  dataFn(props.params)
    .then(data => {
      let fields = [];
      emit('update:columns', data.columns || data);
      if (!isArray(data)) {
        data.columns.push({ type: 'TIMESTAMP', field: data.ts_field_name });
        fields = data.columns.concat(data.tags || []);
      } else {
        fields = data;
      }
      emit('update:tags', data.tags || []);
      const result: any = [];
      if (props.avgFn) {
        result.push({
          field: '*',
          name: '*',
          fieldList: fields,
          result: handleFnParamsFiled(fnMap.AVGFN),
          // fnList: fnMap.AVGN,
          checked: true,
          conditionList: [],
          condition: [],
          fnList: loadAllFns()
        });
      }
      try {
        fields.forEach((item: any) => {
          const fnList = fnMap[getType(item.type, 'result')];
          result.push({
            type: item.type,
            field: `\`${item.field}\``,
            name: item.field,
            fieldList: fields,
            result: handleFnParamsFiled(fnList),
            condition: [
              {
                value: '',
                value1: '',
                key: 1,
                operator: ''
              }
            ],
            checked: true,
            // fnList,
            fnList: loadAllFns(),
            conditionList: conditionMap[getType(item.type)]
          });
        });
      } catch (error) {
        console.log(error);
      }
      console.log('output:result', result);
      emit('update:modelValue', result);
      console.log('output:value', props.modelValue);
    })
    .catch(() => (options.value = []));
}
function handleFnParamsFiled(fnList = []) {
  const result = {
    fn: ''
  };
  fnList.forEach((item: any) => {
    if (item.filters) {
      result.params = {};
      item.filters.forEach(it => {
        result.params[it.field] = it.defaultValue;
      });
    }
  });
  return result;
}
function getType(type, fnType?) {
  if (props.avgFn && fnType == 'result') return 'AVGFN';
  if (!type) return '';
  type = type.replace(/\(\d+\)/, '');
  if (TDengineStringType.includes(type)) return 'STRING';
  if (TDengineNumberType.includes(type)) return 'NUMBER';
  return type;
}
function result(row, index, type = 0) {
  dialog.value = true;
  currentRowIndex.value = index;
  dialogType.value = type;
  row.fnList
    .map(item => item.options)
    .flat(1)
    .map(val => {
      const type = row.type?.toLowerCase().includes('varchar')
        ? 'varchar'
        : row.type?.toLowerCase().includes('nchar')
          ? 'nchar'
          : row.type?.toLowerCase();
      if (!val.supportDatatype.includes(type)) {
        if (val.supportDatatype[0] == 'all' || val.supportDatatype[0] == 'system') {
          val['selectDisable'] = false;
        } else {
          val['selectDisable'] = true;
        }
      } else {
        val['selectDisable'] = false;
      }
    });

  const newRow = deepClone(row);
  // dialogParams = newRow;
  Object.assign(dialogParams, newRow);
  field.value = newRow.name;
}

function confirm() {
  switch (dialogType.value) {
    case 0:
      // eslint-disable-next-line vue/no-mutating-props
      props.modelValue[currentRowIndex.value].result = dialogParams.result;
      break;
    case 1:
      // eslint-disable-next-line vue/no-mutating-props
      props.modelValue[currentRowIndex.value].condition = dialogParams.condition;
      break;
    default:
      break;
  }
  dialog.value = false;
}
//加载官网提供的所有函数
function loadAllFns() {
  let result: any = [];
  result = Object.keys(fnMap).map(key => {
    return {
      label: t(`topic.explorerfns.${fnMapName.get(key)}`),
      options: fnMap[key]
    };
  });
  return result;
}
</script>

<style scoped lang="scss">
.result-set {
  max-height: 300px;

  li {
    display: grid;
    grid-template-columns: 16px auto 100px;
    grid-gap: 10px;
    align-items: center;
    padding: 5px;
  }
}
</style>
