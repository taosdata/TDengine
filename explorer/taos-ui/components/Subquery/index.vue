<template>
  <div>
    <el-form-item :label="t('common.database')" prop="dbName" :rules="rules.dbName">
      <el-select v-model="formData.dbName" class="w-full" filterable placeholder="" @change="dbChange">
        <el-option v-for="item in dbList" :key="item.name" :value="item.name"></el-option>
      </el-select>
    </el-form-item>
    <slot name="db-bottom"></slot>
    <template v-if="level">
      <el-form-item v-if="level == 1" :label="t('stb.stable')" prop="stbName" :rules="rules.stbName">
        <el-select
          v-model="formData.stbName"
          class="w-full"
          placeholder=""
          :disabled="!formData.dbName"
          :default-first-option="true"
          filterable
          :remote-method="searchStable"
          :loading="requestIng"
          remote
          @focus="focus(0)"
        >
          <el-option v-for="item in stableList" :key="item.stable_name" :value="item.stable_name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="level == 2" :label="t('stb.table')" prop="tbName" :rules="rules.tbName">
        <el-select
          v-model="formData.tbName"
          class="w-full"
          placeholder=""
          :disabled="!formData.dbName"
          :default-first-option="true"
          filterable
          :remote-method="searchTable"
          :loading="requestIng"
          remote
          @focus="focus(1)"
        >
          <el-option v-for="item in tableList" :key="item.table_name" :value="item.table_name"></el-option>
        </el-select>
      </el-form-item>
      <template v-if="(formData.stbName || formData.tbName) && fieldSet">
        <el-form-item prop="resultSet" :label="t('topic.fieldSet')">
          <ResultSet
            ref="resultSet"
            v-model:tags="tags"
            v-model:columns="columns"
            v-model="formData.resultSet"
            :avg-fn="avgFn"
            :params="params"
          />
        </el-form-item>
        <el-form-item :label="t('topic.conditionSet')" prop="conditionJson">
          <SqlCondition v-model="formData.conditionJson" top :fields="allFields" parent-field="conditionJson." />
        </el-form-item>
      </template>
      <el-form-item v-if="parttion && level == 1" prop="parttionSet" :label="t('stream.parttionSet')">
        <el-select v-model="formData.parttionSet" class="w-full" placeholder="" :disabled="!formData.stbName">
          <el-option v-for="item in partitionList" :key="item.field" :value="item.field"></el-option>
        </el-select>
      </el-form-item>
      <WindowClause v-if="windowClause" v-model="formData" :column-list="columns" />
    </template>
  </div>
</template>

<script lang="ts" setup>
import { getDbList, searchTable, searchStable } from '../api';
import ResultSet from './resultSet.vue';
import { isArray } from 'utils/validate';
import WindowClause from './windowClause.vue';
import { generateConditionString, Field } from '../SqlCondition/utils';
import { t } from 'locales';
import { ElMessage } from 'element-plus';
import SqlCondition from '../SqlCondition/condition.vue';
import { SubqueryValue } from './type';
import { TDFnType } from 'constants1';

const props = withDefaults(
  defineProps<{
    modelValue: SubqueryValue;
    level?: number;
    fieldSet?: boolean;
    parttion?: boolean;
    windowClause?: boolean;
    avgFn?: boolean;
  }>(),
  {
    modelValue: () => ({
      dbName: '',
      stbName: '',
      tbName: '',
      resultSet: [],
      conditionJson: [],
      tol_val: 0 // Provide a default value for tol_val
    }),
    level: 1,
    fieldSet: false,
    dbConfig: () => ({
      label: t('common.database'),
      filed: 'dbName'
    }),
    stbConfig: () => ({
      label: t('stb.stable'),
      filed: 'stbName'
    }),
    parttion: false,
    windowClause: false,
    avgFn: false
  }
);
const stableList = ref<Recordable[]>([]);
const tableList = ref<Recordable[]>([]);
const dbList = ref<Recordable[]>([]);
const requestIng = ref(false);
const tags = ref<Field[]>([]);
const columns = ref<Field[]>([]);
const params = computed<[string, string, string]>(() => {
  return [props.modelValue.dbName, props.modelValue.stbName, props.modelValue.tbName];
});
const rules = {
  dbName: [{ required: true, message: t('common.requiredTemp', [t('common.database')]), trigger: 'change' }],
  stbName: [{ required: true, message: t('common.requiredTemp', [t('stb.stable')]), trigger: 'change' }],
  tbName: [{ required: true, message: t('common.requiredTemp', [t('stb.table')]), trigger: 'change' }]
};
const partitionList = computed(() => {
  if (props.modelValue.window_type == 'INTERVAL') {
    return tags.value.concat([
      {
        field: 'tbname',
        type: 'STRING'
      }
    ]);
  }
  return [
    {
      field: 'tbname',
      type: 'STRING'
    }
  ];
});
const formData = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emits('update:modelValue', val);
  }
});
const allFields = computed(() => {
  return columns.value.concat(tags.value);
});
const emits = defineEmits(['update:dbList', 'db-change', 'update:modelValue']);

watch(params, () => {
  formData.value.conditionJson = [];
});

getDBList();
function getDBList() {
  getDbList().then(data => {
    dbList.value = data;
    emits('update:dbList', data);
  });
}
function dbChange(val: string) {
  formData.value.stbName = '';
  formData.value.tbName = '';
  emits('db-change', val);
}
function focus(type: number) {
  if (type == 0) {
    !props.modelValue.stbName && handleSearchStable('');
  } else {
    !props.modelValue.tbName && handleSearchTable('');
  }
}
function handleSearchStable(query: string) {
  if (requestIng.value) return;
  requestIng.value = true;
  searchStable(query, props.modelValue.dbName)
    .then(data => {
      stableList.value = data;
    })
    .catch((err: any) => {
      stableList.value = [];
      err.desc && ElMessage.error(err.desc);
    })
    .finally(() => {
      requestIng.value = false;
    });
}
function handleSearchTable(query: string) {
  if (requestIng.value) return;
  requestIng.value = true;
  searchTable(query, props.modelValue.dbName)
    .then(data => {
      tableList.value = data;
    })
    .catch((err: any) => {
      tableList.value = [];
      err.desc && ElMessage.error(err.desc);
    })
    .finally(() => {
      requestIng.value = false;
    });
}

defineExpose({
  generateSql
});
function generateSql() {
  let resultSet: string[] = [];
  const condition = generateConditionString(props.modelValue.conditionJson, allFields.value);
  let isResultSet = false;
  props.modelValue.resultSet.forEach(item => {
    if (!item.checked) return;
    // 处理result
    const result = item.result;
    if (result.fn) {
      isResultSet = true;
      const fnList: TDFnType[] = item.fnList || [];
      const currentFn = fnList.find(ite => ite.label == result.fn);
      const currentFnFilters: Recordable[] = currentFn?.filters || [];
      let otherParmas = item.field;
      if (currentFnFilters.length) {
        if (currentFn?.composeFn) {
          otherParmas = currentFn.composeFn(item.field, result.params);
        } else {
          otherParmas = currentFnFilters
            .reduce(
              (pre, { field }: Recordable) => {
                const value: string[] = result.params[field];
                if (value != undefined) {
                  if (isArray(value)) {
                    pre.push(...value);
                  } else {
                    pre.push(value);
                  }
                }
                return pre;
              },
              [item.field] as string[]
            )
            .join(',');
        }
      }
      resultSet.push(`${result.fn}(${otherParmas})`);
    } else {
      if (!props.avgFn) {
        resultSet.push(item.field);
      }
    }
  });
  const name = props.level == 1 ? props.modelValue.stbName : props.modelValue.tbName;
  let result = '';
  if (!isResultSet && (!resultSet.length || resultSet.length == props.modelValue.resultSet.length)) {
    resultSet = props.avgFn ? ['count(*)'] : ['*'];
  }
  result = `SELECT ${resultSet.join(',')} FROM \`${props.modelValue.dbName}\`.\`${name}\``;
  if (condition) {
    result += ` WHERE ${condition}`;
  }
  return result;
}
</script>

<style scoped lang="scss"></style>
