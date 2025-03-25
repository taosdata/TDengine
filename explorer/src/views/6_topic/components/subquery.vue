<!-- eslint-disable vue/no-mutating-props -->
<template>
  <div>
    <el-form-item :label="dbLabel" :prop="dbFiled" required>
      <el-select v-model="info[dbFiled]" class="w100" filterable placeholder="" @change="dbChange">
        <el-option v-for="item in dbList" :key="item.name" :value="item.name"></el-option>
      </el-select>
    </el-form-item>
    <slot name="db-bottom"></slot>
    <template v-if="level">
      <el-form-item v-if="level == 1" :label="stbLabel" :prop="stbField" required>
        <el-select
          v-model="info[stbField]"
          class="w100"
          placeholder=""
          :disabled="!info[dbFiled]"
          :default-first-option="true"
          filterable
          :remote-method="searchStableData"
          :loading="requestIng"
          remote
          @focus="focus(0)"
        >
          <el-option v-for="item in stableList" :key="item.stable_name" :value="item.stable_name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="level == 2" :label="$t('data.tableName')" prop="tbName" required>
        <el-select
          v-model="info.tbName"
          class="w100"
          placeholder=""
          :disabled="!info[dbFiled]"
          :default-first-option="true"
          filterable
          :remote-method="searchTableData"
          :loading="requestIng"
          remote
          @focus="focus(1)"
        >
          <el-option v-for="item in tableList" :key="item.table_name" :value="item.table_name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="(info[stbField] || info.tbName) && fieldSet" prop="resultSet" :label="$t('topic.fieldSet')">
        <ResultSet
          ref="resultSet"
          v-model:tags="tags"
          v-model:columns="columns"
          v-model="info.resultSet"
          :avg-fn="avgFn"
          :params="params"
        />
      </el-form-item>
      <el-form-item v-if="parttion && level == 1" prop="parttionSet" :label="$t('stream.parttionSet')">
        <el-select
          ref="resultSet"
          v-model:tags="tags"
          v-model="info.parttionSet"
          class="w100"
          placeholder=""
          :disabled="!info[stbField]"
          :params="params"
          multiple
        >
          <el-option v-for="item in partitionList" :key="item.field" :value="item.field"></el-option>
        </el-select>
      </el-form-item>
      <WindowClause v-if="windowClause" :window-clause="info" :column-list="columns" />
    </template>
  </div>
</template>

<script setup lang="ts">
import { getDBListReq } from '@/api/database';
import { searchTable } from '@/api/tables';
import { searchStable } from '@/api/stables';
import ResultSet from './resultSet.vue';
import { isArray } from '@/utils/validate';
import WindowClause from './windowClause.vue';
import { TDengineFnReverseGroup } from '@/const';
const { $error } = inject('globalCustomProperties') as GlobalCustomProperties;

const { t } = useI18n();

const props = defineProps({
  info: {
    type: Object,
    default: () => {
      return {
        db_name: '',
        topic_type: 'DATABASE',
        stbName: '',
        tbName: '',
        resultSet: []
      };
    }
  },
  level: {
    type: Number,
    default: 1
  },
  fieldSet: {
    type: Boolean,
    default: false
  },
  dbConfig: {
    type: Object,
    default: () => ({})
  },
  stbConfig: {
    type: Object,
    default: () => ({})
  },
  parttion: {
    type: Boolean,
    default: false
  },
  windowClause: {
    type: Boolean,
    default: false
  },
  avgFn: {
    type: Boolean,
    default: false
  }
});
const emit = defineEmits(['update:dbList', 'db-change']);

const systemFns = ['DATABASE', 'CLIENT_VERSION', 'SERVER_VERSION', 'SERVER_STATUS'];
const stableList = ref([]);
const tableList = ref([]);
const dbList = ref([]);
const requestIng = ref<boolean>(false);
const tags: any = ref([]);
const columns = ref([]);

const params = computed(() => {
  const result = {
    selected_db: props.info[dbFiled.value]
  };
  if (props.level == 1) {
    result.stableName = props.info[stbField.value];
  } else {
    result.selected_tb = props.info.tbName;
  }
  return result;
});
const dbLabel = computed(() => {
  return props.dbConfig?.label || t('topic.database');
});
const dbFiled = computed(() => {
  return props.dbConfig?.filed || 'db_name';
});
const stbLabel = computed(() => {
  return props.stbConfig?.label || t('topic.stable');
});
const stbField = computed(() => {
  return props.stbConfig?.filed || 'stbName';
});
const partitionList = computed(() => {
  return tags.value.concat([
    {
      field: 'tbname'
    }
  ]);
});

function getDBList() {
  getDBListReq().then(data => {
    dbList.value = data;
    emit('update:dbList', data);
  });
}
function dbChange(val) {
  emit('db-change', val);
}
function focus(type) {
  if (type == 0) {
    !props.info[stbField.value] && searchStableData('');
  } else {
    !props.info.tbName && searchTableData('');
  }
}
function searchStableData(query) {
  if (requestIng.value) return;
  requestIng.value = true;
  searchStable(query, props.info[dbFiled.value])
    .then(data => {
      stableList.value = data;
    })
    .catch(err => {
      stableList.value = [];
      err.desc && $error(err.desc);
    })
    .finally(() => {
      requestIng.value = false;
    });
}
function searchTableData(query) {
  if (requestIng.value) return;
  requestIng.value = true;
  searchTable(query, props.info[dbFiled.value])
    .then(data => {
      tableList.value = data;
    })
    .catch(err => {
      tableList.value = [];
      err.desc && $error(err.desc);
    })
    .finally(() => {
      requestIng.value = false;
    });
}
function getResultSet() {
  let resultSet: any[] = [];
  const conditionSet = [];
  let isResultSet = false;
  props.info.resultSet.forEach(item => {
    if (!item.checked) return;
    // 处理result
    const result = item.result;
    const condition = item.condition.filter(ite => {
      if (['IS NULL', 'IS NOT NULL'].includes(ite.operator)) {
        return ite;
      } else if (['BETWEEN', 'NOT BETWEEN'].includes(ite.operator)) {
        return ite.value && ite.value1;
      } else {
        return ite.operator && ite.value;
      }
    });
    if (result.fn) {
      isResultSet = true;
      const fnList = item.fnList.map(item => item.options).flat(1) || [];
      const currentFn = fnList.find(ite => ite.label == result.fn)?.filters || [];
      let otherParmas = '';
      const isReverse = TDengineFnReverseGroup.includes(result.fn);
      if (currentFn.length) {
        otherParmas = currentFn
          .reduce((pre, { field }) => {
            const value = result.params[field];
            if (value) {
              if (isArray(value)) {
                isReverse ? value.forEach(v => pre.push(JSON.stringify(v))) : pre.push(...value);
              } else {
                isReverse ? pre.push(JSON.stringify(value)) : pre.push(value);
              }
              return pre;
            }
          }, [])
          .join(',');
        if (otherParmas) {
          otherParmas = isReverse ? otherParmas + ',' : ',' + otherParmas;
        }
      }
      if (systemFns.includes(result.fn)) {
        resultSet.push(`${result.fn}()`);
      } else {
        resultSet.push(
          `${result.fn}(${isReverse ? otherParmas + JSON.stringify(item.name) : item.field + otherParmas})`
        );
      }
    } else {
      if (!props.avgFn) {
        resultSet.push(item.field);
      }
    }
    // 处理condition
    if (condition.length) {
      conditionSet.push(
        condition
          .reduce((pre, cur) => {
            if (cur.operator == 'BETWEEN' || cur.operator == 'NOT BETWEEN') {
              pre.push(`${item.field} ${cur.operator} ${cur.value} AND ${cur.value1}`);
            } else if (cur.operator == 'IN' || cur.operator == 'NOT IN') {
              pre.push(`${item.field} ${cur.operator} (${cur.value})`);
            } else {
              pre.push(`${item.field} ${cur.operator} ${cur.value}`);
            }
            return pre;
          }, [])
          .join(' AND ')
      );
    }
  });
  const name = props.level == 1 ? props.info.stbName : props.info.tbName;
  let result = '';
  if (!isResultSet && (!resultSet.length || resultSet.length == props.info.resultSet.length)) {
    resultSet = props.avgFn ? ['count(*)'] : ['*'];
  }
  result = `SELECT ${resultSet.join(',')} FROM \`${props.info[dbFiled.value]}\`.\`${name}\``;
  if (conditionSet.length) {
    result += ` WHERE ${conditionSet.join(' AND ')}`;
  }
  return result;
}

defineExpose({
  getResultSet
});

getDBList();
</script>

<style scoped lang="scss"></style>
