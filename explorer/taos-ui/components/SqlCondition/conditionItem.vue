<template>
  <section class="condition-item">
    <template v-if="isForm">
      <el-form-item :prop="parentField + 'field'" :rules="ruleMap.field" :size="size">
        <el-select v-model="currentValue.field" class="el-width" filterable :placeholder="t('explorer.field')">
          <el-option v-for="item in fields" :key="item.filed" :value="item.field"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :prop="parentField + 'operator'" :rules="ruleMap.operator" :size="size">
        <el-select
          v-model="currentValue.operator"
          class="el-width"
          filterable
          :placeholder="t('explorer.operator')"
          @change="operatorChange"
        >
          <el-option v-for="item in operatorList" :key="item" :value="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item
        v-if="!NoValueOperator.includes(currentValue.operator)"
        :prop="parentField + 'value'"
        :rules="ruleMap.value"
        :size="size"
      >
        <div v-if="ConcatAndOperator.includes(currentValue.operator)" class="between-value-input">
          <el-input v-model="currentValue.value[0]"></el-input>
          AND
          <el-input v-model="currentValue.value[1]"></el-input>
        </div>
        <el-input
          v-else
          v-model="currentValue.value as string"
          class="el-width"
          :type="valueType"
          :placeholder="valuePlaceholder"
        ></el-input>
      </el-form-item>
      <el-form-item v-if="!lasted" :prop="parentField + 'field'" :rules="ruleMap.connector" :size="size">
        <el-radio-group v-model="currentValue.connector" :size="size">
          <el-radio-button v-for="item in connectorList" :key="item" :value="item">{{ item }}</el-radio-button>
        </el-radio-group>
      </el-form-item>
      <el-tooltip effect="light" :content="t('explorer.delRule')" :size="size">
        <el-button class="ml-5px" size="small" icon="delete" @click="emits('del')"></el-button>
      </el-tooltip>
    </template>
    <template v-else>
      <el-select
        v-model="currentValue.field"
        class="el-width"
        filterable
        :placeholder="t('explorer.field')"
        :size="size"
      >
        <el-option v-for="item in fields" :key="item.filed" :value="item.field"></el-option>
      </el-select>
      <el-select
        v-model="currentValue.operator"
        class="el-width ml-5px"
        filterable
        :placeholder="t('explorer.operator')"
        :size="size"
        @change="operatorChange"
      >
        <el-option v-for="item in operatorList" :key="item" :value="item"></el-option>
      </el-select>
      <template v-if="!NoValueOperator.includes(currentValue.operator)">
        <div v-if="ConcatAndOperator.includes(currentValue.operator)" class="between-value-input">
          <el-input v-model="currentValue.value[0]" :size="size" class="mx-5px"></el-input>
          AND
          <el-input v-model="currentValue.value[1]" :size="size" class="ml-5px"></el-input>
        </div>
        <el-input
          v-else
          v-model="currentValue.value as string"
          class="el-width ml-5px"
          :type="valueType"
          :size="size"
          :placeholder="valuePlaceholder"
        ></el-input>
      </template>
      <el-radio-group v-if="!lasted" v-model="currentValue.connector" class="ml-5px" :size="size">
        <el-radio-button v-for="item in connectorList" :key="item" :value="item">{{ item }}</el-radio-button>
      </el-radio-group>

      <el-tooltip effect="light" :content="t('explorer.delRule')">
        <el-button class="ml-5px" plain :size="size" icon="delete" @click="emits('del')"></el-button>
      </el-tooltip>
    </template>
  </section>
</template>

<script lang="ts" setup>
import { conditionMap, NoValueOperator, ConcatAndOperator } from 'constants1/tdengine';
import { getFieldType } from 'utils/tdengine';
import { isArray } from 'utils/validate';
import { conditionItemProps } from './utils';
import { t } from 'locales';

const props = withDefaults(defineProps<conditionItemProps>(), {
  fields: () => [],
  lasted: false,
  parentField: '',
  isForm: false
});
const { fields, lasted, isForm, parentField } = toRefs(props);
const connectorList = ['AND', 'OR'];
const valueType = ref('text');
const size = 'small';
const currentValue = computed({
  get: () => props.modelValue,
  set: val => emits('update:modelValue', val)
});
const emits = defineEmits(['update:modelValue', 'del']);
const ruleMap = {
  field: [{ required: true, message: t('common.requiredTemp', [t('explorer.field')]), trigger: 'change' }],
  operator: [{ required: true, message: t('common.requiredTemp', [t('explorer.operator')]) }],
  value: [
    {
      required: true,
      validator: (_: any, val: string | any[], callback: AnyFunction) => {
        if (isArray(val) && val.length === 2 && val[0] && val[1]) {
          callback();
        } else if (!isArray(val) && val) {
          callback();
        } else {
          callback(new Error(t('common.requiredTemp', [t('common.value')])));
        }
      }
    }
  ],
  connector: [{ required: true, message: t('common.requiredTemp', [t('explorer.connector')]) }]
};
const currentFieldConfig = computed(() => {
  return props.fields.find(item => item.field === currentValue.value.field);
});
const valuePlaceholder = computed(() => {
  if (!currentFieldConfig.value) return t('common.value');
  return t('common.value') + `(${currentFieldConfig.value?.type})`;
});
const operatorList = computed(() => {
  if (currentValue.value.field === '') return [];
  if (!currentFieldConfig.value) return [];
  const parseType = getFieldType(currentFieldConfig.value.type);
  valueType.value = parseType === 'NUMBER' ? 'number' : 'text';
  return conditionMap[parseType as keyof typeof conditionMap];
});

function operatorChange() {
  if (NoValueOperator.includes(currentValue.value.operator)) {
    currentValue.value.value = '';
  }
  if (ConcatAndOperator.includes(currentValue.value.operator)) {
    currentValue.value.value = ['', ''];
  } else if (isArray(currentValue.value.value)) {
    currentValue.value.value = currentValue.value?.toString();
  }
}
</script>

<style scoped lang="scss">
.condition-item {
  position: relative;
  display: flex;
  align-items: center;

  &:deep(.el-form-item--small.el-form-item + .el-form-item--small.el-form-item) {
    margin-left: 5px;
  }

  &:deep(.el-form-item--small.el-form-item) {
    margin-bottom: 0 !important;
  }

  .el-width {
    width: 120px;
  }

  &:deep(.el-input--mini .el-input__inner) {
    height: 24px;
    line-height: 24px;
  }

  .between-value-input {
    display: flex;
    align-items: center;

    .el-input {
      width: 100px;
    }
  }
}
</style>
