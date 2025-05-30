<template>
  <div>
    <div class="form-title">{{ formTitle }}</div>
    <div class="form-wrapper">
      <el-form ref="formIns" label-position="left" label-width="auto" :model="formData">
        <!-- Name -->
        <el-form-item prop="name" :rules="stbNameRule">
          <template #label>
            <div class="flex-start">
              <span>{{ t('common.name') }}</span>
              <el-tooltip
                v-if="!isEdit"
                class="item"
                effect="light"
                :content="t('stb.nameFormatTip')"
                placement="top-start"
              >
                <InfoFilled class="ml-10px w-14px h-14px" />
              </el-tooltip>
            </div>
          </template>
          <el-input v-model="formData.name" size="default" :maxlength="192" :title="formData.name" :disabled="isEdit" />
        </el-form-item>
        <!-- virtual columns -->
        <el-collapse v-if="isVirtual" :model-value="['1']">
          <el-collapse-item :title="t('stb.columns')" name="1">
            <el-form-item
              v-for="(column, index) in formData.columns"
              :key="'column' + index"
              label-width="0"
              :prop="'columns.' + index + '.value'"
              :rules="virtualColumnRule"
            >
              <div class="flex" style="gap: 8px">
                <!-- Field:Type display -->
                <p class="no-wrap" style="display: inline; min-width: 120px" :title="column.type">
                  {{ `${column.field}:${column.type}` }}
                </p>

                <!-- Database Select -->
                <el-select
                  v-model="column.database"
                  style="width: 180px"
                  :placeholder="t('taosuser.tipSelectTarget')"
                  filterable
                  clearable
                  @change="onDatabaseChange(column, index)"
                >
                  <el-option
                    v-for="db in formData.databases"
                    :key="db['node-key']"
                    :label="db.name"
                    :value="db.name"
                  ></el-option>
                </el-select>

                <!-- Table Name Select -->
                <el-select
                  v-model="column.table"
                  style="width: 180px"
                  :props="{
                    checkStrictly: false,
                    emitPath: false,
                    label: 'label',
                    value: 'value'
                  }"
                  :placeholder="t('stb.virtualTableColumn')"
                  :disabled="!column.database"
                  clearable
                  filterable
                  remote
                  :remote-method="remoteTablesForVirtualTableRef(column)"
                >
                  <el-option
                    v-for="option in column.tableNameOptions"
                    :key="option.value"
                    :label="option.label"
                    :value="option.value"
                  ></el-option>
                </el-select>

                <!-- Table Column Cascader -->
                <el-select
                  v-model="column.value"
                  style="width: 180px"
                  :props="{
                    checkStrictly: true,
                    emitPath: false,
                    label: 'label',
                    value: 'value'
                  }"
                  :placeholder="t('stb.virtualTableColumn')"
                  :disabled="!column.table || !column.database"
                  clearable
                  filterable
                  @focus="onColumnSelect(column)"
                >
                  <el-option
                    v-for="option in column.cascaderOptions"
                    :key="option.value"
                    :label="option.label"
                    :value="option.value"
                  ></el-option>
                </el-select>

                <!-- Confirm Button (edit mode) -->
                <el-button
                  v-if="isEdit"
                  :disabled="!column.value"
                  icon="check"
                  @click="virtualColumnSourceChange(column)"
                ></el-button>
              </div>
            </el-form-item>
          </el-collapse-item>
        </el-collapse>
        <!-- Tags -->
        <el-collapse :model-value="['1']">
          <el-collapse-item :title="t('stb.tags')" name="1">
            <el-form-item
              v-for="(tag, index) in formData.tags"
              :key="'tag' + index"
              label-width="0"
              :prop="'tags.' + index + '.value'"
              :rules="tagRule"
              ><el-input v-model="tag.value" size="default" :placeholder="t('stb.tagValue')" :title="tag.value">
                <template #prepend>
                  <p class="no-wrap" :title="tag.type">
                    {{ `${tag.field}:${tag.type}` }}
                  </p>
                </template>
                <template v-if="isEdit" #append>
                  <el-button :disabled="!tag.value" icon="check" @click="tagValueChange(tag)"></el-button>
                </template> </el-input
            ></el-form-item>
          </el-collapse-item>
        </el-collapse>

        <div class="flex-center">
          <!-- Comfirm Btn -->
          <el-button
            v-if="!isEdit"
            class="submit-btn"
            size="default"
            :loading="loading"
            :disabled="loading"
            type="primary"
            @click="handleCreateTable"
            >{{ t('common.create') }}</el-button
          >
          <el-button :disabled="loading" class="submit-btn" size="default" @click="cancel">{{
            t('common.cancel')
          }}</el-button>
        </div>
      </el-form>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { CreateSubTbProps, SubTbTagStruct, CreateSubTbForm } from './props';
import { t } from 'locales';
import { generateCreateSubTableSql, stbNameRule } from './utils';
import { VariableTableColumnType } from 'constants1/index';
import { ElMessage, FormInstance } from 'element-plus';
import {
  getDbList,
  getSubtbInitStruct,
  getSubtbCurrentStruct,
  getValidTablesForVirtualTableRef,
  getValidColumnsForVirtualTableRef,
  createTableReq,
  changeTableStruct,
  changeStbStructData
} from '../../api';

import { escapeSpecialChar } from '../../../utils/tdengine';

const props = withDefaults(defineProps<CreateSubTbProps>(), {
  isEdit: false
});
const { isEdit } = toRefs(props);
const formData = reactive<CreateSubTbForm>({
  name: '',
  columns: [],
  tags: [],
  stbTmpl: props.stbName
});
const formIns = ref<FormInstance | null>(null);
const currentDBName = computed(() => props.dbData?.name);
const isVirtual = computed(() => props.isVirtual);
const loading = ref(false);
const formTitle = computed(() => {
  if (!isEdit.value) {
    return t('stb.createTableUse', [formData.stbTmpl]);
  } else {
    return t('stb.editTb', [formData.name]);
  }
});
const virtualColumnRule = [
  {
    required: true,
    message: t('common.requiredTemp', [t('stb.virtualTableColumn')]),
    trigger: 'blur'
  }
];
const tagRule = [
  {
    required: true,
    message: t('common.requiredTemp', [t('stb.tagValue')]),
    trigger: 'blur'
  }
];
const emits = defineEmits(['success', 'cancel']);

defineExpose({
  generateSql: () => generateCreateSubTableSql(formData, currentDBName.value, isVirtual)
});
setFormData();
async function setFormData() {
  if (props.isVirtual) {
    try {
      formData.databases = await getDbList();
    } catch (error) {
      ElMessage.error(t('msg.getDbListFailed'));
      formData.databases = [];
    }
  }
  const promise = props.isEdit
    ? getSubtbCurrentStruct(props.dbData.name, props.stbName!, props.tbName!)
    : getSubtbInitStruct(props.dbData.name, props.stbName!);
  const data = await promise;
  formData.name = data.name;
  if (props.isVirtual) {
    formData.isVirtual = true;
    formData.columns = data.columns
      .filter((_, index) => index !== 0)
      .map(column => {
        column.database = column.database || props.dbData.name;
        return column;
      });
  }
  formData.tags = data.tags;
}

function remoteTablesForVirtualTableRef(column) {
  return async (query: string) => {
    if (loading.value) return;
    if (query == column.value && query != '') {
      console.log('remoteTablesForVirtualTableRef: same query, skip');
      return;
    }
    loading.value = true;
    try {
      const tables = await getValidTablesForVirtualTableRef(column.database, column.type, query);
      column.tableNameOptions = tables.map(table => ({
        label: table,
        value: table
      }));
    } catch (error) {
      ElMessage.error(t('msg.getTableListFailed'));
    } finally {
      loading.value = false;
    }
  };
}
async function onColumnSelect(column) {
  if (loading.value) return;
  loading.value = true;
  try {
    const columns = await getValidColumnsForVirtualTableRef(column.database, column.table, column.type);
    column.cascaderOptions = columns.map(col => ({
      label: col.field,
      value: col.field
    }));
  } catch (error) {
    ElMessage.error(t('msg.getTableColumnsFailed'));
  } finally {
    loading.value = false;
  }
}
function handleCreateTable() {
  if (loading.value || !formIns.value) return;
  formIns.value.validate().then(() => {
    handleData();
    createTableReq(formData, currentDBName.value).then(() => {
      ElMessage.success(t('msg.createSuccess'));
      emits('success');
      emits('cancel');
    });
  });
}
function handleData() {
  formData.tags = formData.tags.filter(item => item.value);
  formData.columns = formData.columns.filter(item => item.value);
}
// 当修改虚拟表结构的列时，列的来源发生变化的
function virtualColumnSourceChange(tag: SubTbTagStruct) {
  const isString = VariableTableColumnType.some(item => tag.type.startsWith(item));
  const value = isString ? `'${escapeSpecialChar(tag.value)}'` : tag.value;
  const params: changeStbStructData = {
    operation: 'set column',
    first_field: `\`${column.field}\` = ${value}`
  };
  updateData(params);
}
// 当修改表结构的tag时，tag的value发生变化的
function tagValueChange(tag: SubTbTagStruct) {
  const isString = VariableTableColumnType.some(item => tag.type.startsWith(item));
  const value = isString ? `'${escapeSpecialChar(tag.value)}'` : tag.value;
  const params: changeStbStructData = {
    operation: 'set tag',
    first_field: `\`${tag.field}\` = ${value}`
  };
  updateData(params);
}
function updateData(params: changeStbStructData) {
  if (loading.value) return;
  loading.value = true;
  changeTableStruct(params, formData.name, currentDBName.value)
    .then(() => {
      ElMessage.success(t('msg.updateSuccess'));
      emits('success');
    })
    .finally(() => {
      loading.value = false;
    });
}
function cancel() {
  emits('cancel');
}
</script>
<style lang="scss" scoped>
.form-title {
  margin-bottom: 20px;
  font-size: 24px;
  font-weight: 400;
}

.form-wrapper {
  width: 1000px;
  padding-right: 18px;

  --group-prepend: 200px;
  --group-append: 150px;
}

.submit-btn {
  width: 30%;
  margin-top: 20px;

  &.submit-btn {
    margin-right: 10px;
  }
}

:deep(.el-collapse) {
  border-top: 0;
}

:deep(.el-collapse-item__header) {
  font-size: 18px;
  border-bottom: none !important;
}

:deep(.el-collapse-item__wrap) {
  border-bottom: none !important;
}
</style>
