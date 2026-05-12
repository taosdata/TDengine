<script setup lang="ts">
import { WarningFilled } from '@element-plus/icons-vue';
import { t } from 'locales';
import { applyExpressionMode, isExpressionDisabled, shouldShowGeneratorWarning } from './mappingExpressionState';
import type { MappingColumnOption, TableRow } from './type';

const props = defineProps<{
  row: TableRow;
  mappingTypes: string[];
  mappingcolumns: MappingColumnOption[];
  exprformat: string;
  exprexpression: string;
  onDefaultValueInput: (name: string, value: string | number | null | undefined, range?: (bigint | number)[]) => void;
}>();

const emit = defineEmits<{
  changed: [];
  cleared: [row: TableRow];
}>();

function onExpressionChange() {
  emit('changed');
}

function onExprnameChange() {
  applyExpressionMode(props.row);
  emit('changed');
}

function onSubTableNameBlur() {
  const row = props.row;
  if (typeof row.Expression === 'string') {
    row.Expression = row.Expression.trim();
  }
  emit('changed');
}

function onExpressionCleared() {
  emit('cleared', props.row);
}
</script>

<!-- eslint-disable vue/no-mutating-props -->
<template>
  <div class="box-expression">
    <template v-if="row.Name === 'SubTableName'">
      <el-input v-model="row.Expression" size="default" :placeholder="exprformat" @blur="onSubTableNameBlur" />
    </template>
    <template v-else>
      <div class="expression-row">
        <el-select v-model="row.exprname" size="default" class="mapping-rule-select" @change="onExprnameChange">
          <el-option v-for="item in mappingTypes" :key="item" :label="item" :value="item" />
        </el-select>

        <el-select
          v-if="row.exprname === 'mapping' || row.exprname === 'sum' || row.exprname === 'join'"
          v-model="row.Expression"
          :placeholder="t('dataIn.transformer.coltip')"
          :clearable="row.exprname === 'mapping'"
          size="default"
          filterable
          class="mapping-rule-expression"
          :multiple="row.exprname !== 'mapping'"
          @clear="onExpressionCleared"
        >
          <el-option v-for="val in mappingcolumns" :key="val.label" :value="val.value" :label="val.label" />
        </el-select>
        <el-input
          v-else
          :key="'expr'"
          v-model="row.Expression"
          class="mapping-rule-expression"
          :placeholder="
            row.exprname === 'format'
              ? exprformat
              : row.exprname === 'expr'
                ? exprexpression
                : row.exprname === 'value'
                  ? t('dataIn.transformer.valuetip')
                  : ''
          "
          size="default"
          :disabled="isExpressionDisabled(row)"
          @change="onExpressionChange"
        />

        <el-tooltip
          v-if="shouldShowGeneratorWarning(row)"
          :content="t('dataIn.transformer.generatorPkWarning')"
          placement="top"
          effect="light"
          :open-delay="0"
        >
          <el-icon style="margin-left: 4px; color: #f0a020; cursor: pointer; flex-shrink: 0">
            <WarningFilled />
          </el-icon>
        </el-tooltip>

        <el-input
          v-if="row.exprname === 'join'"
          :key="'exprjoin'"
          v-model="row.joinwith"
          size="default"
          class="mapping-rule-extra"
        >
          <template #prepend>with</template>
        </el-input>
        <el-input
          v-else-if="row.exprname === 'mapping' && row.dataRange"
          :key="'default-value-of-' + row.Name"
          v-model="row.default"
          size="default"
          type="number"
          :placeholder="t('dataIn.transformer.defaultValuePlaceholder')"
          :maxlength="row.dataRange[2]"
          class="mapping-rule-extra"
          @blur="onDefaultValueInput(row.Name, row.default, row.dataRange)"
        />
        <el-select
          v-else-if="row.exprname === 'mapping' && row.dataType === 'BOOL'"
          v-model="row.default"
          :placeholder="t('dataIn.transformer.defaultValuePlaceholder')"
          size="default"
          class="mapping-rule-extra"
        >
          <el-option label="true" value="true" />
          <el-option label="false" value="false" />
          <el-option label="null" value="null" />
        </el-select>
        <el-input
          v-else-if="row.exprname === 'mapping' && row.dataType"
          v-model="row.default"
          size="default"
          :placeholder="t('dataIn.transformer.defaultValuePlaceholder')"
          class="mapping-rule-extra"
        />
      </div>
    </template>
    <div v-if="row.defaultValueError" class="default-value-error">
      {{ row.defaultValueError }}
    </div>
  </div>
</template>
<!-- eslint-enable vue/no-mutating-props -->

<style lang="scss" scoped>
.box-expression {
  display: flex;
  flex-direction: column;
  width: 100%;
  min-width: 0;

  .expression-row {
    display: flex;
    flex-wrap: nowrap;
    align-items: center;
    width: 100%;
    min-width: 0;
  }

  .mapping-rule-select {
    flex: 0 0 110px;
    width: 110px;
    margin-right: 5px;

    :deep(.el-select),
    :deep(.el-select__wrapper) {
      width: 100%;
      min-width: 0;
    }
  }

  .mapping-rule-expression {
    flex: 1 1 0;
    min-width: 0;
    width: auto;

    :deep(.el-select),
    :deep(.el-select__wrapper),
    :deep(.el-input),
    :deep(.el-input__wrapper) {
      width: 100%;
      min-width: 0;
    }
  }

  .mapping-rule-extra {
    flex: 0 0 100px;
    width: 100px;
    margin-left: 5px;
    min-width: 0;

    :deep(.el-input),
    :deep(.el-input__wrapper),
    :deep(.el-select),
    :deep(.el-select__wrapper) {
      width: 100%;
      min-width: 0;
    }
  }

  .default-value-error {
    width: 100%;
    margin-top: 5px;
    font-size: 12px;
    line-height: 1;
    color: #ff4949;
    text-align: right;
  }
}
</style>
