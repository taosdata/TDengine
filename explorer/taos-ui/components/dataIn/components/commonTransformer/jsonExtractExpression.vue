<template>
  <div class="json-wrap">
    <el-form ref="jsonFormRef" :model="parseruleForm" :rules="rules" size="default">
      <div class="form-row">
        <div class="field-group">
          <span class="field-label">depth</span>
          <el-input-number
            v-model="parseruleForm.depth"
            class="depth-input"
            size="default"
            :controls="false"
            :min="0"
          />
        </div>
        <div class="field-group">
          <span class="field-label">keep</span>
          <el-switch v-model="parseruleForm.keep" size="default" />
        </div>
        <el-input
          v-model="parseruleForm.expression"
          class="expression-input"
          :placeholder="t('dataIn.transformer.jsonPlaceholder')"
        />
      </div>
    </el-form>
  </div>
</template>

<script setup lang="ts">
import { t } from 'locales';
import { transformerState } from './util';
import { JsonParseExtractType } from './type';
import { cloneDeep } from 'lodash-es';
const props = withDefaults(
  defineProps<{
    ruleForm: Recordable;
    isViewable: boolean;
  }>(),
  {
    ruleForm: () => {
      return {
        depth: undefined,
        keep: false,
        expression: null
      };
    }
  }
);
const parseruleForm = reactive(props.ruleForm);
const isValid = ref(true);
const rules = reactive({
  depth: [
    {
      trigger: 'blur'
    }
  ],
  keep: [
    {
      trigger: 'blur'
    }
  ],
  expression: [
    {
      trigger: 'blur'
    }
  ]
});

const jsonFormRef = ref();

const emit = defineEmits(['update:ruleForm']);

watch(
  () => transformerState.jsonExtractListType,
  val => {
    const middleObj = cloneDeep(val) as JsonParseExtractType;
    console.log('watch:', 'middleObj', middleObj, transformerState.jsonExtractListType);
  },
  {
    deep: true
  }
);

watch(parseruleForm, newData => {
  emit('update:ruleForm', newData);
});

onMounted(() => {
  if (transformerState.jsonExtractListType) {
    const middleobj = cloneDeep(transformerState.jsonExtractListType) as JsonParseExtractType;
    console.log('onMounted:', 'middleObj', middleobj, transformerState.convertExpressList);
  }
});

function submit() {
  jsonFormRef.value.validate((valid: boolean) => {
    if (valid) {
      isValid.value = true;
      const jsonExpr = {} as JsonParseExtractType;
      Object.keys(parseruleForm)
        .filter(key => {
          parseruleForm[key];
        })
        .forEach(item => {
          jsonExpr[item] = parseruleForm[item].toString().trim();
        });
      transformerState.jsonExtractListType = jsonExpr;
      return true;
    } else {
      isValid.value = false;
      return false;
    }
  });
}

defineExpose({
  submit,
  isValid
});
</script>
<style lang="scss" scoped>
.json-wrap {
  width: 100%;
  min-width: 0;
}

.form-row {
  display: flex;
  align-items: center;
  gap: 4px;
  min-width: 0;
}

.field-group {
  display: inline-flex;
  align-items: center;
  gap: 4px;

  .field-label {
    padding: 0 6px;
    line-height: 30px;
    color: #909399;
    background-color: #f5f7fa;
    border: 1px solid #dcdfe6;
    border-radius: 4px;
    white-space: nowrap;
  }
}

.depth-input {
  width: 60px;
}

.expression-input {
  flex: 1;
  min-width: 0;
}
</style>
