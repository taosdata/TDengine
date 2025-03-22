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
  getSubtbInitStruct,
  getSubtbCurrentStruct,
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
  tags: [],
  stbTmpl: props.stbName
});
const formIns = ref<FormInstance | null>(null);
const currentDBName = computed(() => props.dbData?.name);
const loading = ref(false);
const formTitle = computed(() => {
  if (!isEdit.value) {
    return t('stb.createTableUse', [formData.stbTmpl]);
  } else {
    return t('stb.editTb', [formData.name]);
  }
});
const tagRule = [
  {
    required: true,
    message: t('common.requiredTemp', [t('stb.tagValue')]),
    trigger: 'blur'
  }
];
const emits = defineEmits(['success', 'cancel']);

defineExpose({
  generateSql: () => generateCreateSubTableSql(formData, currentDBName.value)
});
setFormData();
function setFormData() {
  (props.isEdit
    ? getSubtbCurrentStruct(props.dbData.name, props.stbName!, props.tbName!)
    : getSubtbInitStruct(props.dbData.name, props.stbName!)
  ).then(data => {
    formData.name = data.name;
    formData.tags = data.tags;
  });
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
