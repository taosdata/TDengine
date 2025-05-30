<template>
  <div class="w-full">
    <div class="flex-center input-row">
      <section class="column-prepend-btn">
        <el-select
          v-model="type"
          size="default"
          :disabled="isEdit || isTimestamp"
          default-first-option
          filterable
          :placeholder="t('common.dataType')"
          @change="typeChange"
        >
          <el-option v-for="item in dataType" :key="item" :value="item"></el-option>
        </el-select>
        <el-input
          v-if="VariableTableColumnType.includes(type)"
          v-model="fieldLength"
          class="custom-length"
          size="default"
          type="number"
          :min="8"
          clearable
          :max="VariableTableColumnTypeMaxLenthMap[type as ColumnTypeMaxLenMapKey]"
          @change="processTypeLength"
        ></el-input>
      </section>

      <el-input
        v-model="field"
        size="default"
        class="flex-1"
        :maxlength="64"
        :disabled="inputDisabled"
        :placeholder="placeholder"
        @blur="validName"
        @change="fieldChange"
      >
        <template #append>
          <template v-if="isAdd">
            <el-button size="small" icon="close" @click="emits('cancel')"></el-button>
            <el-button
              size="small"
              :disabled="props.loading || !field"
              icon="check"
              @click="emits('confirm')"
            ></el-button>
          </template>
          <template v-else>
            <section v-if="!isTimestamp">
              <el-icon style="vertical-align: middle"><DArrowLeft /></el-icon>
              <!-- Database Select -->
              <el-select
                v-model="database"
                style="width: 180px; margin: 0 auto !important"
                :placeholder="t('dataIn.placeholders.chooseTargetDbTip')"
                filterable
                clearable
              >
                <el-option v-for="db in databases" :key="db['node-key']" :label="db.name" :value="db.name"></el-option>
              </el-select>

              <!-- Table Name Select -->
              <el-select
                v-model="table"
                style="width: 180px; margin: 0 auto !important"
                :props="{
                  checkStrictly: false,
                  emitPath: false,
                  label: 'label',
                  value: 'value'
                }"
                :placeholder="t('stb.virtualTableFromTableName')"
                :disabled="!database"
                clearable
                filterable
                remote
                :remote-method="remoteTablesForVirtualTableRef"
              >
                <el-option
                  v-for="option in localStore.tableNameOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                ></el-option>
              </el-select>

              <!-- Table Column Cascader -->
              <el-select
                v-model="value"
                style="width: 40px; margin: 0 4px 0 0 !important"
                :props="{
                  checkStrictly: true,
                  emitPath: false,
                  label: 'label',
                  value: 'value'
                }"
                :placeholder="t('stb.virtualTableColumn')"
                :disabled="!table || !database"
                clearable
                filterable
                @focus="onColumnSelect(column)"
              >
                <el-option
                  v-for="option in localStore.cascaderOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                ></el-option>
              </el-select>
            </section>
            <el-button icon="minus" :disabled="isTimestamp" @click="emits('minusColumn')"></el-button>
            <el-button v-if="!isEdit" :disabled="!field" icon="plus" @click="emits('addColumn')"></el-button>
            <el-button v-else :disabled="btnDisabled" icon="check" @click="emits('typeChange')"></el-button>
          </template>
        </template>
      </el-input>
    </div>
    <p v-if="errorText" class="error-text">{{ errorText }}</p>
  </div>
</template>

<script lang="ts" setup>
import { VariableTableColumnType, TDengineDataType, VariableTableColumnTypeMaxLenthMap } from 'constants1/index';
import { validTDKeywords } from 'utils/validate';
import { VirtualColumnProps } from '../props';
import { t } from 'locales';
import { getValidTablesForVirtualTableRef, getValidColumnsForVirtualTableRef } from '../../../api';

type ColumnTypeMaxLenMapKey = keyof typeof VariableTableColumnTypeMaxLenthMap;

const defaultModel = {
  field: '',
  type: 'INT',
  database: '',
  table: '',
  value: '',
  length: 8
};

const props = withDefaults(defineProps<VirtualColumnProps>(), {
  modelValue: () => {},
  isEdit: false,
  isTag: false,
  isAdd: false,
  loading: false,
  placeholder: t('stb.columnName'),
  isTimestamp: false,
  isCanSetPrimaryKey: false,
  canMoveToTag: true,
  databases: []
});
const minTypeLength = 8;
const errorText = ref('');
let fieldLength = ref(props.modelValue.length);
const dataType = computed(() => (props.isTag ? TDengineDataType.concat(['JSON']) : TDengineDataType));
const databases = computed(() => props.databases || []);
const inputDisabled = computed(() => (props.isAdd || props.isTag ? false : props.isEdit));
const btnDisabled = computed(() =>
  props.isAdd || props.isTag
    ? !props.modelValue.field
    : !props.modelValue.field || !VariableTableColumnType.includes(props.modelValue.type)
);

const localStore = reactive({
  tableNameOptions: [],
  cascaderOptions: []
});

const loading = ref(false);

const emits = defineEmits([
  'update:modelValue',
  'cancel',
  'confirm',
  'minusColumn',
  'addColumn',
  'moveTag',
  'typeChange'
]);

// 响应式状态
const localModel = reactive({ ...defaultModel, ...props.modelValue });

// 同步更新
watch(
  () => props.modelValue,
  newVal => {
    Object.assign(localModel, newVal);
  },
  { immediate: true }
);

// 触发父组件更新
watch(localModel, newVal => {
  emits('update:modelValue', { ...newVal });
});
// 暴露响应式属性
const { type, database, table, field, value } = toRefs(localModel);

console.log(
  'type:',
  type?.value,
  'field:',
  field?.value,
  'database:',
  database?.value,
  'table:',
  table?.value,
  'column:',
  value?.value
);
const fieldChange = () => {
  errorText.value = '';
};

async function remoteTablesForVirtualTableRef(query) {
  if (loading.value) return;
  if (query == value.value && query != '') {
    console.log('remoteTablesForVirtualTableRef: same query, skip');
    return;
  }
  loading.value = true;
  try {
    const tables = await getValidTablesForVirtualTableRef(database.value, type.value, query);
    console.log('remoteTablesForVirtualTableRef: tables', tables);
    localStore.tableNameOptions = tables.map(table => ({
      label: table,
      value: table
    }));
  } catch (error) {
    ElMessage.error(t('msg.getTableListFailed', [database.value, type.value, query, error]));
  } finally {
    loading.value = false;
    // valueChange();
  }
}
async function onColumnSelect() {
  if (loading.value) return;
  loading.value = true;
  try {
    const columns = await getValidColumnsForVirtualTableRef(database.value, table.value, type.value);
    localStore.cascaderOptions = columns.map(col => ({
      label: col.field,
      value: col.field
    }));
  } catch (error) {
    ElMessage.error(t('msg.getTableColumnsFailed', [database.value, table.value, type.value, error]));
  } finally {
    loading.value = false;
    // valueChange();
  }
}
function processTypeLength(val: number | string) {
  if (!val) return (fieldLength = minTypeLength);
  val = Number(val);
  fieldLength = Math.min(
    Math.max(val, minTypeLength),
    VariableTableColumnTypeMaxLenthMap[type as ColumnTypeMaxLenMapKey]
  );
  // valueChange();
}

function typeChange(val: string) {
  if (VariableTableColumnType.includes(val)) {
    fieldLength = Math.max(fieldLength.value, minTypeLength);
  }
}
function validName() {
  if (validTDKeywords(props.modelValue.field)) {
    errorText.value = t('explorer.tdKewordTip', [props.modelValue.field]);
  }
}
</script>

<style scoped lang="scss">
$height: 32px;

.input-row {
  width: 100%;
  margin-top: var(--group-margin-top);
}

.error-text {
  padding: 0;
  padding-bottom: 5px;
  margin: 0;
  font-size: 12px;
  color: #ff4949;
  text-align: left;
}

.column-prepend-btn {
  display: flex;
  flex-shrink: 0;

  .custom-length {
    flex-shrink: 0;
    width: calc(var(--group-prepend) * 0.35);
    border-right: none;

    &:deep(.el-input__wrapper) {
      height: $height;
      border: 1px solid var(--el-border-color);
      border-right: none;
      border-top-right-radius: 0;
      border-bottom-right-radius: 0;
      box-shadow: unset;
    }
  }
}

.flex-center {
  &:deep(.el-select__wrapper) {
    height: $height;
    border: 1px solid var(--el-border-color);
    border-right: none;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
    box-shadow: unset;
  }

  &:deep(.el-input__wrapper) {
    border-top-left-radius: 0;
    border-bottom-left-radius: 0;
  }

  &:deep(.el-input-group__append) {
    padding: 0 5px;
    margin: unset;

    .el-button {
      padding: 5px;
      margin: 0;

      & + .el-button {
        margin-left: 0;
      }
    }
  }
}

.input-row .primary-key-checkbox.el-tag {
  height: $height;
  border-color: var(--el-border-color);
  border-right: none;
  border-radius: unset;
}

.column-width {
  flex-shrink: 0;
  width: 110px;
  min-width: 110px;

  &:deep(.el-select__wrapper) {
    border-radius: unset;
  }
}
</style>
