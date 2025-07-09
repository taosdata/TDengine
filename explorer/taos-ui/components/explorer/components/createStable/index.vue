<template>
  <div class="stb-create">
    <div v-if="props.showTitle" class="form-title">{{ formTitle }}</div>
    <el-form
      ref="formIns"
      class="form-wrapper"
      size="default"
      label-position="left"
      label-width="auto"
      :model="formData"
    >
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
        <el-input v-model="formData.name" :disabled="isEdit" :maxlength="192" :title="formData.name"> </el-input>
      </el-form-item>
      <el-collapse v-model="activeNames">
        <el-collapse-item name="1" :title="t('stb.columns')">
          <el-form-item
            v-for="(column, index) in formData.columns"
            :key="'column' + index"
            label-width="0"
            :prop="'columns.' + index + '.field'"
            :rules="columnRule"
          >
            <ColumnItem
              v-model="formData.columns[index]"
              :version="props.version"
              :is-edit="isEdit"
              :is-timestamp="index == 0"
              :is-can-set-primary-key="index == 1"
              @minus-column="minusColumn(index)"
              @add-column="addColumn(index)"
              @type-change="typeChange(column, 'column', index)"
              @move-tag="moveToTag(index)"
            />
          </el-form-item>
          <!-- 添加用的column -->
          <ColumnItem
            v-if="currentEditType == 'column' && currentData"
            v-model="currentData"
            :version="props.version"
            :loading="loading"
            :is-add="true"
            class="mb-20px"
            @cancel="
              currentEditType = '';
              currentData = null;
            "
            @confirm="add"
            @move-tag="addMoveToTag"
          />
          <el-button
            v-if="isEdit"
            class="w-full"
            size="default"
            plain
            :disabled="currentEditType == 'column'"
            icon="plus"
            @click="addColumn()"
          ></el-button>
        </el-collapse-item>
        <el-collapse-item name="2" :title="t('stb.tags')">
          <!-- Tag Section -->
          <!-- 占位元素 -->
          <el-form-item
            v-for="(column, index) in formData.tags"
            :key="'tag' + index"
            label-width="0"
            :prop="'tags.' + index + '.field'"
            :rules="tagRule"
          >
            <ColumnItem
              :key="'tag' + index"
              v-model="formData.tags[index]"
              :version="props.version"
              :is-edit="isEdit"
              :is-tag="true"
              :placeholder="t('stb.tagName')"
              @minus-column="minusColumn(index, 'tag')"
              @add-column="addColumn(index, 'tag')"
              @type-change="typeChange(column, 'tag', index)"
            />
          </el-form-item>

          <!-- 添加用的tag -->
          <ColumnItem
            v-if="currentEditType == 'tag' && isEdit && currentData"
            v-model="currentData"
            :version="props.version"
            class="mb-20px"
            :loading="loading"
            :is-add="true"
            :is-tag="true"
            :placeholder="t('stb.tagName')"
            @cancel="
              currentEditType = '';
              currentData = null;
            "
            @confirm="add"
          />
          <el-button
            v-if="isEdit && CanAddNewTag"
            class="w-full"
            size="default"
            plain
            :disabled="currentEditType == 'tag'"
            icon="plus"
            @click="addColumn(0, 'tag')"
          ></el-button>
        </el-collapse-item>
      </el-collapse>

      <!-- Comfirm Btn -->
      <div class="flex-center">
        <el-button
          v-if="!isEdit"
          class="submit-btn"
          :disabled="loading"
          size="default"
          :loading="loading"
          type="primary"
          @click="handleCreateStable"
          >{{ t('common.create') }}</el-button
        >
        <el-button class="submit-btn" :disabled="loading" size="default" @click="cancel">{{
          t('common.cancel')
        }}</el-button>
      </div>
    </el-form>
  </div>
</template>

<script lang="ts" setup>
import ColumnItem from './columnItem.vue';
// import { cloneDeep } from 'lodash-es';
import { type_default_version_gte_3300, generateCreateStbSql } from './utils';
import { ColumnStruct, CreateStableProps, CreateStableForm } from '../props';
import { isGte3300, stbNameRule, columnRule, tagRule } from '../utils';
import { t } from 'locales';
import { ElMessage, ElMessageBox, FormInstance } from 'element-plus';
import { composeType } from 'utils/tdengine';
import { getStableStructReq, createStableReq, changeStableStruct, changeStbStructData } from '../../../api';

const props = withDefaults(defineProps<CreateStableProps>(), {
  showTitle: true,
  columnsArray: () => [],
  isEdit: false
});
const version_gte_3300 = computed(() => isGte3300(props.version));
const columnNewField = computed(() =>
  version_gte_3300.value
    ? type_default_version_gte_3300
    : {
        TIMESTAMP: {},
        INT: {}
      }
);
const isEdit = toRef(props, 'isEdit');
const formData = reactive<CreateStableForm>({
  name: '',
  columns: [
    { type: 'TIMESTAMP', field: '', length: 8, length2: 0, ...columnNewField.value.TIMESTAMP },
    { type: 'INT', field: '', length: 8, length2: 0, ...columnNewField.value.INT }
  ],
  tags: [{ type: 'INT', field: '', length: 8, length2: 0 }]
});
const formIns = ref<FormInstance | null>(null);
const currentEditType = ref('');
const currentData = ref<ColumnStruct | null>(null);
const loading = ref(false);
const activeNames = ref(['1', '2']);
const currentSelectedDb = computed(() => props.dbData?.name);
const emits = defineEmits(['success', 'cancel']);

// const tagDataClone: TagStruct[] = cloneDeep(formData.tags);
const formTitle = computed(() => {
  if (props.isEdit) {
    return t('stb.editStable', [formData.name]);
  } else {
    return t('stb.createStbInDb', [currentSelectedDb.value]);
  }
});
const CanAddNewTag = computed(() => formData.tags.length < 128);
const columnStruct = computed(() => {
  return {
    field: '',
    type: 'INT',
    length: 8,
    length2: 0,
    ...(version_gte_3300.value ? type_default_version_gte_3300.INT : {})
  };
});
if (props.isEdit) {
  setFormData();
}

function setFormData() {
  getStableStructReq(currentSelectedDb.value, props.stbName!).then(data => {
    formData.name = props.stbName!;
    formData.columns = data.columns;
    formData.columns.forEach(c => {
      c.origin_length = c.length;
    });
    formData.tags = data.tags;
    formData.tags.forEach(t => {
      t.origin_field = t.field;
      t.origin_length = t.length;
    });
  });
}

// 类型修改
async function typeChange(data: ColumnStruct, type: 'column' | 'tag') {
  // 不是修改状态就不处理
  if (!props.isEdit) return;

  try {
    if (data.length > 0 && data.origin_length !== data.length) {
      const params: changeStbStructData = {
        operation: 'modify ' + type,
        first_field: data.origin_field || data.field,
        second_field: composeType(data)
      };
      loading.value = true;
      await changeStableStruct(params, formData.name, currentSelectedDb.value);
    }

    if (type === 'tag' && data.origin_field !== data.field) {
      const params = {
        operation: 'rename tag',
        first_field: data.origin_field || '',
        second_field: `\`${data.field}\``
      };
      loading.value = true;
      await changeStableStruct(params, formData.name, currentSelectedDb.value);
    }

    if (loading.value === true) {
      ElMessage.success(t('msg.modifySuccess'));
      setFormData();
    }
  } finally {
    loading.value = false;
  }
}
// 当修改时更新数据的接口，与新增无关
function updateData(params: changeStbStructData) {
  loading.value = true;
  changeStableStruct(params, formData.name, currentSelectedDb.value)
    .then(() => {
      ElMessage.success(t('msg.modifySuccess'));
    })
    .catch(() => false)
    .finally(() => {
      // 无论修改成功或失败都应该刷新数据
      loading.value = false;
      setFormData();
    });
}
function addColumn(index?: number, type: 'column' | 'tag' = 'column') {
  if (!props.isEdit) {
    const dataList = type == 'column' ? formData.columns : formData.tags;
    if (index == undefined) {
      index = dataList.length - 1;
    }
    dataList.splice(index + 1, 0, { ...columnStruct.value });
  } else {
    currentEditType.value = type;
    currentData.value = { ...columnStruct.value };
  }
}
function minusColumn(index: number, type: 'column' | 'tag' = 'column') {
  const dataList = type == 'column' ? formData.columns : formData.tags;
  if (!props.isEdit) return dataList.splice(index, 1);
  const data = dataList[index];
  ElMessageBox.confirm(
    t('msg.confirmTemp', {
      operate: t('common.delete'),
      name: data.field
    }),
    t('status.warning'),
    {
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      type: 'warning'
    }
  )
    .then(() => {
      const params = {
        operation: 'drop ' + type,
        first_field: data.field
      };
      updateData(params);
    })
    .catch(() => {});
}
function moveToTag(index: number) {
  if (formData.columns.length > 1) {
    const column = formData.columns.splice(index, 1)[0];
    formData.tags.push(column);
  }
}
function addMoveToTag() {
  currentEditType.value = 'tag';
}

function handleCreateStable() {
  if (loading.value || !formIns.value) return;
  formIns.value.validate().then(() => {
    processData();
    createStableReq(formData, currentSelectedDb.value)
      .then(() => {
        ElMessage.success(t('msg.createSuccess'));
        emits('success', formData.name);
        emits('cancel');
      })
      .catch(() => {});
  });
}
defineExpose({
  generateSql: () => generateCreateStbSql(formData, currentSelectedDb.value)
});
function processData() {
  formData.columns = formData.columns.filter(item => item.field);
  formData.tags = formData.tags.filter(item => item.field);
}
// 修改状态时，确定后发送请求添加数据
function add() {
  if (!currentData.value) return;
  const params: changeStbStructData = {
    operation: 'add ' + currentEditType.value,
    first_field: currentData.value.field,
    second_field: composeType(currentData.value)
  };
  if (version_gte_3300.value && currentEditType.value == 'column') {
    params.other = ` ENCODE '${currentData.value.encode}' COMPRESS '${currentData.value.compress}' LEVEL '${currentData.value.level}'`;
  }
  currentData.value = null;
  currentEditType.value = '';
  updateData(params);
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
  width: 1100px;

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

.stb-create {
  &:deep(.el-collapse) {
    border-top: 0;
  }

  &:deep(.el-collapse-item__header) {
    font-size: 18px;
    border-bottom: none !important;
  }

  &:deep(.el-collapse-item__wrap) {
    border-bottom: none !important;
  }
}
</style>
