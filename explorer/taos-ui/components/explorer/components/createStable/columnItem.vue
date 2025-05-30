<template>
  <div class="w-full">
    <div class="flex-center input-row">
      <section class="column-prepend-btn">
        <el-select
          v-model="currentValue.type"
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
          v-if="VariableTableColumnType.includes(currentValue.type)"
          v-model="currentValue.length"
          class="custom-length"
          size="default"
          type="number"
          :min="8"
          clearable
          :max="VariableTableColumnTypeMaxLenthMap[currentValue.type as ColumnTypeMaxLenMapKey]"
          @change="processTypeLength"
        ></el-input>
      </section>
      <template v-if="!isTag && isVerGte3300">
        <el-tooltip placement="top" effect="light" :open-delay="100" :content="t('stb.encode')">
          <el-select
            v-model="currentValue.encode"
            size="default"
            default-first-option
            default-value="simple8b"
            placeholder="ENCODE"
            class="column-width"
            clearable
            @change="valueChange"
          >
            <el-option
              v-for="item in getStbEncodeAndCompressListByType(currentValue.type)['encodeList']"
              :key="item.value"
              v-bind="item"
            ></el-option>
          </el-select>
        </el-tooltip>
        <el-tooltip placement="top" effect="light" :open-delay="100" :content="t('stb.compress')">
          <el-select
            v-model="currentValue.compress"
            size="default"
            default-first-option
            default-value="lz4"
            placeholder="COMPRESS"
            class="column-width"
            clearable
            @change="valueChange"
          >
            <el-option
              v-for="item in getStbEncodeAndCompressListByType(currentValue.type)['compressList']"
              :key="item.value"
              v-bind="item"
            ></el-option>
          </el-select>
        </el-tooltip>
        <el-tooltip placement="top" effect="light" :open-delay="100" :content="t('common.level')">
          <el-select
            v-model="currentValue.level"
            size="default"
            default-first-option
            placeholder="LEVEL"
            class="column-width"
            clearable
            @change="valueChange"
          >
            <el-option v-for="item in levelList" :key="item.value" v-bind="item"></el-option>
          </el-select> </el-tooltip
        ><el-tag
          v-if="isCanSetPrimaryKey && !isEdit"
          size="large"
          effect="plain"
          type="info"
          class="primary-key-checkbox"
        >
          <el-checkbox
            v-model="currentValue.primaryKey"
            :disabled="isEdit || parmaryKeyType.findIndex(item => item.value.includes(currentValue.type)) == -1"
            @change="valueChange"
            >PRIMARY KEY</el-checkbox
          >
        </el-tag>
      </template>
      <el-input
        v-model="currentValue.field"
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
              :disabled="props.loading || !currentValue.field"
              icon="check"
              @click="emits('confirm')"
            ></el-button>
          </template>
          <template v-else>
            <el-button icon="minus" :disabled="isTimestamp" @click="emits('minusColumn')"></el-button>
            <el-button
              v-if="!isEdit"
              :disabled="!currentValue.field"
              icon="plus"
              @click="emits('addColumn')"
            ></el-button>
            <el-button v-else :disabled="btnDisabled" icon="check" @click="emits('typeChange')"></el-button>
          </template>
          <el-button v-if="canMoveToTag && !isEdit" size="small" :disabled="isTimestamp" @click="emits('moveTag')">
            <Icon name="tag" style="width: 14px; height: 12px"></Icon>
          </el-button>
        </template>
      </el-input>
    </div>
    <p v-if="errorText" class="error-text">{{ errorText }}</p>
  </div>
</template>

<script lang="ts" setup>
import { VariableTableColumnType, TDengineDataType, VariableTableColumnTypeMaxLenthMap } from 'constants1/index';
import { parmaryKeyType, levelList, getStbEncodeAndCompressListByType } from './utils';
import { hasOwnProperty } from 'utils/validate';
import { validTDKeywords } from 'utils/validate';
import { ColumnItemProps } from '../props';
import { isGte3300 } from '../utils';
import { t } from 'locales';

type ColumnTypeMaxLenMapKey = keyof typeof VariableTableColumnTypeMaxLenthMap;
const props = withDefaults(defineProps<ColumnItemProps>(), {
  modelValue: () => ({}),
  isEdit: false,
  isTag: false,
  isAdd: false,
  loading: false,
  placeholder: t('stb.columnName'),
  isTimestamp: false,
  isCanSetPrimaryKey: false,
  canMoveToTag: true
});

let minTypeLength = 8;
const errorText = ref('');
const dataType = computed(() => (props.isTag ? TDengineDataType.concat(['JSON']) : TDengineDataType));
const isVerGte3300 = computed(() => isGte3300(props.version));
const canMoveToTag = computed(() => {
  console.log('canMoveToTag', props);
  if (props.isTag) return false;
  return props.canMoveToTag;
});
const inputDisabled = computed(() => (props.isAdd || props.isTag ? false : props.isEdit));
const btnDisabled = computed(() =>
  props.isAdd || props.isTag
    ? !props.modelValue.field
    : !props.modelValue.field || !VariableTableColumnType.includes(props.modelValue.type)
);
const currentValue: any = reactive({
  field: '',
  primaryKey: false,
  type: 'TIMESTAMP',
  length: 8,
  compress: 'lz4',
  encode: 'simple8b',
  level: 'medium'
});

const emits = defineEmits([
  'update:modelValue',
  'cancel',
  'confirm',
  'minusColumn',
  'addColumn',
  'moveTag',
  'typeChange'
]);

const valueChange = () => {
  const updateValue: any = {};
  for (const key in props.modelValue) {
    updateValue[key] = currentValue[key];
  }
  emits('update:modelValue', updateValue);
};
const fieldChange = () => {
  errorText.value = '';
  valueChange();
};

watch(
  () => props.modelValue,
  newval => {
    for (const key in newval) {
      currentValue[key] = newval[key];
    }
    if (props.isEdit && newval.origin_length > 8) {
      minTypeLength = newval.origin_length;
    }
  },
  { immediate: true }
);

function processTypeLength(val: number | string) {
  if (!val) return (currentValue.length = minTypeLength);
  val = Number(val);
  currentValue.length = Math.min(
    Math.max(val, minTypeLength),
    VariableTableColumnTypeMaxLenthMap[currentValue.type as ColumnTypeMaxLenMapKey]
  );
  valueChange();
}

function typeChange(val: string) {
  if (VariableTableColumnType.includes(val)) {
    currentValue.length = Math.max(currentValue.length, minTypeLength);
  }
  if (hasOwnProperty(currentValue, 'encode')) {
    const data = getStbEncodeAndCompressListByType(currentValue.type);
    const { defaultEncode, defaultCompress } = data;
    currentValue.encode = defaultEncode;
    currentValue.compress = defaultCompress;
    currentValue.level = 'medium';
    // 如果不支持 primary key
    if (currentValue.primaryKey && parmaryKeyType.findIndex(item => item.value.includes(currentValue.type)) == -1) {
      currentValue.primaryKey = false;
    }
  }
  valueChange();
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
