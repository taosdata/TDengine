<template>
  <div class="w-full">
    <el-input v-model="search" :placeholder="t('topic.searchFieldTip')" clearable> </el-input>
    <el-table tooltip-effect="dark" class="w-full" :data="currentValue" size="small">
      <el-table-column width="40">
        <template #header>
          <el-checkbox v-model="checkAll" :indeterminate="isIndeterminate" @change="handleCheckAllChange"></el-checkbox>
        </template>
        <template #default="scope">
          <el-checkbox v-model="scope.row.checked" @change="handleCheckChange"></el-checkbox>
        </template>
      </el-table-column>
      <el-table-column :label="t('common.name')" show-overflow-tooltip prop="name" width="120">
        <template #default="{ row }">
          <span>{{ row.name }}</span>
          <el-tooltip v-if="row.name == '*'" effect="light" :content="t('topic.allFieldExplanation')">
            <el-icon class="ml-5px" size="small">
              <InfoFilled />
            </el-icon>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column prop="type" :label="t('common.type')" show-overflow-tooltip width="120"> </el-table-column>
      <el-table-column prop="result" :label="t('common.function')" min-width="110">
        <template #default="{ row }">
          <Result
            v-if="row.fnList"
            v-model="row.result"
            :config="row.fnList"
            :current-field="row.field"
            :field-list="allFieldList"
          />
        </template>
      </el-table-column>
    </el-table>
    <Pagination v-model:current-page="page" size="small" :total="total" :page-size="pageSize" />
  </div>
</template>

<script lang="ts" setup>
import Result from './result.vue';
import { getSubtbCurrentStruct, getStableStructReq } from '../api';
import {
  NumbericFn,
  TimeSeriesFn,
  StringFn,
  AggregationFn,
  SelectorFn,
  StreamSupportFnMap,
  TDFnType
} from 'constants1';
import { isArray } from 'utils/validate';
import { composeType, getFieldType } from 'utils/tdengine';
import { CheckboxValueType } from 'element-plus';
import { t } from 'locales';
import type { TDFnDataStruct } from './type';

const props = withDefaults(
  defineProps<{
    params: [string, string, string];
    modelValue: Recordable[];
    avgFn: boolean;
  }>(),
  {
    params: () => ['', '', ''],
    value: () => [],
    avgFn: false
  }
);
const total = ref(0);
const pageSize = ref(10);
const page = ref(1);
const options = ref<Recordable[]>([]);
const search = ref('');
const allFieldList = ref<Recordable[]>([]);
const checkAll = ref(true);
const isIndeterminate = ref(false);
const currentValue = computed(() => {
  return props.modelValue
    .filter(item => item.name.includes(search.value))
    .slice((page.value - 1) * pageSize.value, page.value * pageSize.value);
});
const fnMap = computed(() => {
  const NUMBER = NumbericFn;
  const STRING = StringFn;
  const AVGFN = AggregationFn.concat(SelectorFn, TimeSeriesFn).sort((a, b) => a.label.localeCompare(b.label));
  if (props.avgFn) {
    return {
      ...StreamSupportFnMap,
      GENERALAVG: StreamSupportFnMap.AVGFN.filter((item: TDFnType) => !item.applicableDataTypes)
    };
  }
  return {
    NUMBER,
    STRING,
    AVGFN,
    GENERALAVG: []
  };
});
const emits = defineEmits(['update:modelValue', 'update:columns', 'update:tags']);

watch(
  () => props.params,
  () => {
    getData();
  },
  {
    immediate: true,
    deep: true
  }
);
watch(currentValue, () => {
  handleCheckChange();
});

function handleCheckChange() {
  const checkedCount = currentValue.value.filter(item => item.checked).length;
  checkAll.value = checkedCount === currentValue.value.length;
  isIndeterminate.value = checkedCount > 0 && checkedCount < currentValue.value.length;
}
function handleCheckAllChange(val: CheckboxValueType) {
  currentValue.value.forEach(row => {
    row.checked = val;
  });
  isIndeterminate.value = false;
  checkAll.value = val as boolean;
}
function getData() {
  if (!props.params[0] || (!props.params[1] && !props.params[2])) return emits('update:modelValue', []);
  page.value = 1;
  const dataFn = props.params[1] ? getStableStructReq : getSubtbCurrentStruct;
  search.value = '';
  checkAll.value = true;
  dataFn(...props.params)
    .then((data: any) => {
      if (!isArray(data)) {
        allFieldList.value = data.columns.concat(data.tags || []);
      } else {
        allFieldList.value = data as Recordable[];
      }
      emits('update:columns', data.columns || data);
      emits('update:tags', data.tags || []);
      const result = [];
      if (props.avgFn) {
        result.push({
          field: '*',
          name: '*',
          result: handleFnParamsFiled(fnMap.value.GENERALAVG),
          fnList: fnMap.value.GENERALAVG,
          checked: true
        });
      }
      try {
        allFieldList.value.forEach(item => {
          const fnList = fnMap.value[getFieldType(item.type) as keyof typeof fnMap.value];
          if (!fnList) return;
          result.push({
            type: composeType({
              type: item.type,
              length: item.length
            }),
            field: `\`${item.field}\``,
            name: item.field,
            result: handleFnParamsFiled(fnList),
            checked: true,
            length: item.length,
            fnList
          });
        });
      } catch (error) {
        console.log(error);
      }
      total.value = result.length;
      emits('update:modelValue', result);
    })
    .catch(() => (options.value = []));
}
function handleFnParamsFiled(fnList: TDFnType[] = []): TDFnDataStruct {
  const result: TDFnDataStruct = {
    fn: ''
  };
  fnList.forEach(item => {
    if (item.filters) {
      item.filters.forEach((it: Recordable) => {
        if (!result.params) {
          result.params = {};
        }
        result.params[it.field] = it.defaultValue;
      });
    }
  });
  console.log(fnList);
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
