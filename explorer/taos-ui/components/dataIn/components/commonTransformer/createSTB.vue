<template>
  <div class="create-stb">
    <el-form
      ref="formRef"
      :model="state.stable_form"
      :rules="state.rules"
      label-position="left"
      label-width="150px"
      @submit.prevent
    >
      <el-form-item prop="name" class="name_input">
        <template #label>
          <span>{{ t('common.name') }}</span>
          <el-tooltip class="item" effect="light" :content="t('stb.nameFormatTip')" placement="top-start">
            <el-icon style="margin-left: 10px" class="el-icon-info"></el-icon>
          </el-tooltip>
        </template>
        <el-input v-model="state.stable_form.name" size="default" :maxlength="192" :title="state.stable_form.name">
        </el-input>
      </el-form-item>
    </el-form>
    <el-collapse v-model="state.activeNames">
      <el-collapse-item name="1" :title="t('stb.columns')">
        <div v-for="(column, index) in state.stable_form.columns" :key="'column' + index" class="flex-center input-row">
          <el-select
            v-model="column.type"
            size="default"
            default-first-option
            :disabled="index == 0"
            class="column-prepend-btn"
            @change="() => handleTypeChange(column, index)"
          >
            <el-option v-for="item in handleTypeList('dataType')" :key="item" :label="item" :value="item"></el-option>
          </el-select>
          <el-input-number
            v-if="VariableTableColumnType.includes(column.type) || TwoVariableTableColumnType.includes(column.type)"
            v-model="column.length"
            size="default"
            :min="1"
            :max="column.type == 'NCHAR' ? 4093 : 65517"
            label="Length"
            controls-position="right"
            class="column-width-110"
            @change="newVal => handleChange(newVal, index)"
          ></el-input-number>
          <el-input-number
            v-if="TwoVariableTableColumnType.includes(column.type)"
            v-model="column.length2"
            size="default"
            default-value=0
            :min="1"
            :max="column.type == 'NCHAR' ? 4093 : 65517"
            label="Length"
            controls-position="right"
            class="column-width-110"
            @change="newVal => handleChange2(newVal, index)"
          ></el-input-number>
          <el-input
            v-model="column.field"
            size="default"
            :maxlength="64"
            :placeholder="t('stb.columnName')"
            style="min-width: 60px"
          >
          </el-input>
          <el-tag
            v-if="index == 1 && version_gt_3300 && activeType == 'sqlCreate'"
            effect="plain"
            type="info"
            size="large"
          >
            <el-checkbox
              v-model="column.primaryKey"
              :disabled="parmaryKeyType.findIndex(item => column.type.startsWith(item.value)) == -1"
              >PRIMARY KEY</el-checkbox
            >
          </el-tag>
          <el-tooltip
            v-if="version_gt_3300 && activeType == 'sqlCreate'"
            placement="top"
            effect="light"
            :open-delay="100"
            :content="t('stb.encode')"
          >
            <el-select
              v-model="column.encode"
              size="default"
              default-first-option
              default-value="simple8b"
              placeholder="ENCODE"
              class="column-width-110"
              clearable
            >
              <el-option
                v-for="item in handleEncodeList(column.type)['encodeList']"
                :key="item.value"
                v-bind="item"
              ></el-option>
            </el-select>
          </el-tooltip>
          <el-tooltip
            v-if="version_gt_3300 && activeType == 'sqlCreate'"
            placement="top"
            effect="light"
            :open-delay="100"
            :content="t('stb.compress')"
          >
            <el-select
              v-model="column.compress"
              size="default"
              default-first-option
              default-value="lz4"
              placeholder="COMPRESS"
              class="column-width-110"
              clearable
            >
              <el-option
                v-for="item in handleEncodeList(column.type)['compressList']"
                :key="item.value"
                v-bind="item"
              ></el-option>
            </el-select>
          </el-tooltip>
          <el-tooltip
            v-if="version_gt_3300 && activeType == 'sqlCreate'"
            placement="top"
            effect="light"
            :open-delay="100"
            :content="t('stb.level')"
          >
            <el-select
              v-model="column.level"
              size="default"
              default-first-option
              placeholder="LEVEL"
              class="column-width-110"
              clearable
            >
              <el-option v-for="item in levelList" :key="item.value" v-bind="item"></el-option>
            </el-select>
          </el-tooltip>
          <span class="action-btn">
            <el-button icon="Minus" size="default" :disabled="!index" @click="minusColumn(index)"></el-button>
            <!-- <el-button icon="Plus" size="default" @click="addColumn"></el-button> -->
            <el-tooltip :content="t('stb.clickColumnTip')">
              <el-button size="default" :disabled="!index" @click="removeToTag(index)">
                <Icon :name="'tag'" class="console-tree-icon" style="width: 18px; height: 18px"></Icon>
              </el-button>
            </el-tooltip>
          </span>
        </div>
        <el-button
          icon="Plus"
          size="default"
          type="primary"
          plain
          style="width: 100%; margin-top: 18px"
          @click="addColumn"
        ></el-button>
      </el-collapse-item>
      <el-collapse-item name="2" :title="t('stb.tags')">
        <div v-for="(column, index) in state.stable_form.tags" :key="'column' + index" class="flex-center input-row">
          <el-select v-model="column.type" size="default" default-first-option class="column-prepend-btn">
            <el-option v-for="item in handleTypeList('tagType')" :key="item" :label="item" :value="item"></el-option>
          </el-select>
          <el-input-number
            v-if="VariableTableColumnType.includes(column.type)"
            v-model="column.length"
            size="default"
            :min="1"
            :max="column.type == 'NCHAR' ? 4093 : 16382"
            label="Length"
            controls-position="right"
            class="column-width-110"
            @change="newVal => tagLengthChange(newVal, index)"
          ></el-input-number>
          <el-input v-model="column.field" size="default" :maxlength="64" :placeholder="t('stb.tagName')">
            <template #append>
              <el-button icon="Minus" @click="minusTags(index)"></el-button>
              <!-- <el-button icon="Plus" @click="addTags"></el-button> -->
            </template>
          </el-input>
        </div>
        <el-button
          icon="Plus"
          size="default"
          type="primary"
          plain
          style="width: 100%; margin-top: 18px"
          @click="addTags"
        ></el-button>
      </el-collapse-item>
    </el-collapse>
    <div class="buttons">
      <el-button type="primary" size="default" @click="submit">
        {{ t('common.create') }}
      </el-button>
      <el-button size="default" @click="close">
        {{ t('common.cancel') }}
      </el-button>
    </div>
  </div>
</template>
<script setup lang="ts">
import { cloneDeep } from 'lodash-es';
import {
  parmaryKeyType,
  storageCompression,
  levelList,
  groupOne,
  groupTwo,
  groupThree,
  groupFour,
  groupFive
} from '../../../explorer/components/createStable/utils';
import { VariableTableColumnType, TDengineDataType, TwoVariableTableColumnType } from 'constants1/index';
import { instance } from 'config';
import { compareVersion } from 'utils/tdengine';
import { t } from 'locales';
import { supportTransform, transformerState } from './util';
import { createStableReq } from 'components/api';
import { getDataInProps } from 'components/dataIn/model/useDataIn';
import { SpbTopParseType, TopParseType } from './type';

const dataInProps = getDataInProps();
const props = defineProps<{
  activeType: string;
  database: string;
}>();

const state = reactive({
  dataType: TDengineDataType,
  tagType: TDengineDataType.concat(['JSON']),
  storageCompression: storageCompression,
  levelList: levelList,
  column_item: {
    type: 'INT',
    field: '',
    value: '',
    length: 8,
    encode: 'simple8b',
    compress: 'lz4',
    level: 'medium'
  },
  column_item_ts: {
    type: 'TIMESTAMP',
    field: '',
    value: '',
    length: 8,
    encode: 'delta-i',
    compress: 'lz4',
    level: 'medium',
    primaryKey: false
  },

  stable_form: {
    name: '',
    ts_field_name: '',
    rollup: '',
    columns: [] as any[],
    tags: [] as any[]
  },
  rules: {
    name: [
      {
        required: true,
        message: t('dataIn.enterTip') + ' ' + t('common.name'),
        trigger: 'blur'
      },
      {
        validator: (_: any, value: string | string[], callback: (arg0: Error | undefined) => void) => {
          callback(value.indexOf('.') != -1 ? new Error(t('formatWrong')) : undefined);
        },
        trigger: 'blur'
      }
    ]
  },
  activeNames: ['1', '2'],
  VariableTableColumnType,
  templateDataType: [] as string[]
});

const formRef = ref();

const emit = defineEmits(['create-stable-succ', 'close', 'create-template-stable-succ']);

const version_gt_3300 = computed(() => compareVersion(instance.version, '>3.3.0.0'));

watch(
  () => props.activeType,
  type => {
    if (type == 'templateCreate') {
      // 模版创建初始值UI
      initTemplateColumns();
    } else {
      initColumns();
    }
  },
  {
    immediate: true
  }
);
function initColumns() {
  let arr = transformerState.stbDefaultColumns as any;
  if (transformerState.stbDefaultColumns.length > 0) {
    arr = arr.map((item: { localType: string; name: any }) => {
      let type = item.localType.toUpperCase();
      type = type.startsWith('TIMESTAMP') ? type.split('(')[0] : type;
      return {
        field: item.name,
        type: type,
        encode: handleEncodeList(type)['defaultEncode'],
        compress: handleEncodeList(type)['defaultCompress'],
        level: 'medium'
      };
    });
    arr.unshift(cloneDeep(state.column_item_ts));
    state.stable_form.columns = arr;
    state.stable_form.tags[0] = cloneDeep(state.column_item);
  } else {
    state.stable_form.columns[0] = cloneDeep({ ...state.column_item_ts });
    state.stable_form.columns[1] = cloneDeep(state.column_item);
    state.stable_form.tags[0] = cloneDeep(state.column_item);
  }
}
function initTemplateColumns() {
  const column_item = {
    type: 'TIMESTAMP',
    field: 'ts',
    value: '',
    length: 8,
    primaryKey: false
  };
  if (JSON.stringify(transformerState.s_model) == '{}') {
    state.stable_form.name = '';
    state.stable_form.columns[0] = cloneDeep(column_item);
    state.stable_form.columns[1] = cloneDeep(state.column_item);
    state.stable_form.tags[0] = cloneDeep(state.column_item);
  } else {
    const s_model = cloneDeep(transformerState.s_model) as any;
    state.stable_form.name = s_model.name;
    state.stable_form.columns = s_model.columns.map((item: { name: string }) => ({ ...item, field: item.name }));
    state.stable_form.tags = s_model.tags.map((item: { name: string }) => ({ ...item, field: item.name }));
  }

  const arr = transformerState.stbDefaultColumns;

  // 动态获取字段
  state.templateDataType = arr.map((item: any) => {
    return `\${${item.name}}`;
  });
}
function handleChange(newVal: any, index: number) {
  state.stable_form.columns[index]['length'] = newVal;
}
function handleChange2(newVal: any, index: number) {
  state.stable_form.columns[index]['length2'] = newVal;
}
function tagLengthChange(newVal: any, index: number) {
  state.stable_form.tags[index]['length'] = newVal;
}
function minusColumn(index: number) {
  if (state.stable_form.columns.length > 1) {
    state.stable_form.columns.splice(index, 1);
  }
  // 是主键列
  if (index == 1) {
    handPrimarykeyCol(index);
  }
}
function minusTags(index: number) {
  if (state.stable_form.tags.length > 1) {
    state.stable_form.tags.splice(index, 1);
  }
}

function addTags() {
  state.stable_form.tags.push(cloneDeep(state.column_item));
}
function addColumn() {
  state.stable_form.columns.push(cloneDeep(state.column_item));
}
function removeToTag(index: number) {
  if (state.stable_form.columns.length > 1) {
    const column = state.stable_form.columns.splice(index, 1)[0];
    state.stable_form.tags.push(cloneDeep(column));
  }
  // 是主键列
  if (index == 1) {
    handPrimarykeyCol(index);
  }
}
function handPrimarykeyCol(index: number) {
  state.stable_form.columns[index]['primaryKey'] = false;
}
function handleEncodeList(type: string) {
  if (!type) return state.storageCompression.empty;
  if (groupOne.includes(type)) {
    return state.storageCompression.groupOne;
  } else if (groupTwo.includes(type)) {
    return state.storageCompression.groupTwo;
  } else if (groupThree.includes(type)) {
    return state.storageCompression.groupThree;
  } else if (groupFour.findIndex(item => type.startsWith(item)) !== -1) {
    return state.storageCompression.groupFour;
  } else if (groupFive.includes(type)) {
    return state.storageCompression.groupFive;
  } else {
    return state.storageCompression.groupSix;
  }
}
function handleTypeChange(column: { type: string; primaryKey: any }, index: number) {
  const data = handleEncodeList(column.type);
  const { defaultEncode, defaultCompress } = data;
  state.stable_form.columns[index]['encode'] = defaultEncode;
  state.stable_form.columns[index]['compress'] = defaultCompress;
  state.stable_form.columns[index]['level'] = 'medium';
  // 如果不支持 primary key
  if (index == 1 && column.primaryKey && parmaryKeyType.findIndex(item => column.type.startsWith(item.value)) == -1) {
    state.stable_form.columns[index]['primaryKey'] = false;
  }
}
function handleTypeList(name: keyof typeof state) {
  if (props.activeType === 'sqlCreate') {
    return state[name];
  } else {
    return (state[name] as string[]).concat(state.templateDataType);
  }
}
function submit() {
  if (props.activeType == 'sqlCreate') {
    createStable();
  } else {
    createTemplateStable();
  }
}
function close() {
  emit('close');
}

async function createStable() {
  formRef.value?.validate(async (valid: boolean) => {
    if (!valid) return false;
    if (valid) {
      const { tags, columns } = state.stable_form;
      for (let i = 0; i < columns.length; i++) {
        const element = columns[i];
        if (!element.field) {
          return ElMessage.warning(t('dataIn.enterTip') + ' ' + t('stb.columnName'));
        }
      }
      for (let i = 0; i < tags.length; i++) {
        const element = tags[i];
        if (!element.field) {
          return ElMessage.warning(t('dataIn.enterTip') + ' ' + t('stb.tagName'));
        }
      }
      if (!version_gt_3300.value) {
        state.stable_form.columns = state.stable_form.columns.map(item => {
          return {
            ...item,
            encode: '',
            compress: '',
            level: ''
          };
        });
      }

      await createStableReq(state.stable_form, props.database).then(() => {
        ElMessage.success(t('msg.createSuccess'));
        emit('create-stable-succ', state.stable_form.name);
      });
    }
  });
}
async function createTemplateStable() {
  formRef.value?.validate(async (valid: boolean) => {
    if (!valid) return false;
    if (valid) {
      const { name, columns, tags } = state.stable_form;
      const newColumns = columns.map(col => {
        return {
          name: col.field,
          length: col.length,
          type: col.type + (VariableTableColumnType.includes(col.type) ? `(${col.length})` : TwoVariableTableColumnType.includes(col.type) ? `(${col.length},${col.length2})` : '')
        };
      });
      const newTags = tags.map(col => {
        return {
          name: col.field,
          length: col.length,
          type: col.type + (VariableTableColumnType.includes(col.type) ? `(${col.length})` : '')
        };
      });
      const s_model = {
        name,
        columns: newColumns,
        tags: newTags
      };
      let parserData;
      if (supportTransform.is_sparkplugb) {
        const topparse = transformerState?.topParse as SpbTopParseType | null;
        const samples = topparse?.samples;
        parserData = {
        parser: {
            parse: transformerState?.topParse?.parser?.parse,
            s_model: s_model,
            mutate: transformerState.transformExtractParseData ? [transformerState.transformExtractParseData] : []
          },

          samples: samples
        };
      } else {
        const topparse = transformerState?.topParse as TopParseType | null;
        const input = topparse?.input;
        parserData = {
        parser: {
            parse: transformerState?.topParse?.parser?.parse,
            s_model: s_model,
            mutate: transformerState.transformExtractParseData ? [transformerState.transformExtractParseData] : []
          },

          input: input
        };
      }

      const result = await dataInProps.transform.api.getStabelParser(parserData);
      if (result && Object.hasOwnProperty.call(result, 'code')) {
        ElMessage.error(result.message || result.desc);
        return;
      }
      transformerState.s_model = s_model;
      emit('create-template-stable-succ', state.stable_form.name);
    }
  });
}
</script>
<style lang="scss" scoped>
.create-stb {
  .column-prepend-btn {
    flex-shrink: 0;
    width: 150px;
  }

  .column-width-110 {
    flex-shrink: 0;
    width: 90px;
    min-width: 50px;
  }

  .input-row {
    margin-top: 18px;
  }

  :deep(.el-collapse) {
    border-top: 0;
  }

  :deep(.el-form-item__content) {
    display: flex;
  }

  :deep(.el-collapse-item__header) {
    font-size: 18px;
    border-bottom: none !important;
  }

  :deep(.el-collapse-item__wrap) {
    border-bottom: none !important;
  }

  :deep(.el-input-number__decrease) {
    height: 16px;
  }

  :deep(.el-input-number__increase) {
    height: 16px;
  }

  :deep(.el-input) {
    .el-input__inner {
      height: 32px !important;
    }
  }

  :deep(.el-input-group__prepend) {
    width: 150px;
    padding-left: 15px;
  }

  :deep(.flex-center .el-select .el-input__inner) {
    border-color: #dcdfe6;
    border-left: none;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }

  :deep(.flex-center .el-input .el-input__inner) {
    border-color: #dcdfe6;
    border-top-left-radius: 0;
    border-bottom-left-radius: 0;
  }

  :deep(.flex-center.el-select:first-of-type .el-input__inner) {
    border-right: none;
    border-left: 1px solid #dcdfe6;
  }

  :deep(.el-input.is-disabled .el-input__inner),
  :deep(.el-input-group__append),
  :deep(.el-input-group__prepend) {
    color: #606266;
    background-color: unset;

    .el-button.is-disabled,
    .el-button.is-disabled:hover,
    .el-button.is-disabled:focus {
      background-color: transparent;
      border-color: transparent;
    }
  }

  .action-btn {
    display: flex;
    margin-left: 10px;

    .el-button + .el-button {
      margin-left: 0;
      border-left-style: none;
    }
  }

  :deep(.el-tag) {
    border-left: none;
  }

  .buttons {
    display: flex;
    align-items: center;
    justify-content: center;

    :deep(.el-button) {
      width: 60px;
    }
  }
}
</style>
