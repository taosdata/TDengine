<template>
  <div class="split-expression">
    <el-form ref="splitFormRef" :model="localeRuleForm" :rules="rules" size="default">
      <el-form-item prop="sep">
        <el-input v-model="localeRuleForm.sep" placeholder="," class="split-item" :disabled="isViewable">
          <template #prepend>seperator</template>
        </el-input>
      </el-form-item>
      <el-form-item prop="n">
        <el-input v-model="localeRuleForm.n" placeholder="3" class="split-item" type="number" :disabled="isViewable">
          <template #prepend>number</template>
        </el-input>
      </el-form-item>
      <!-- <el-form-item prop="names" style="display: none">
        <el-input
          v-model="localeRuleForm.names"
          placeholder="value1,value2,value3"
          class="split-item"
          :disabled="isViewable"
        >
          <template #prepend>names</template>
        </el-input>
      </el-form-item> -->
    </el-form>
  </div>
</template>
<script setup lang="ts">
import { cloneDeep } from 'lodash-es';
import { transformerState } from './util';
import { SplitExpresListType } from './type';
import { t } from 'locales';

const props = withDefaults(
  defineProps<{
    ruleForm: Recordable;
    isViewable: boolean;
  }>(),
  {
    ruleForm: () => {
      return {
        sep: '',
        n: ''
        // names: ''
      };
    }
  }
);
const localeRuleForm = reactive(props.ruleForm);
const isValid = ref(true);
const rules = reactive({
  n: [
    {
      required: true,
      trigger: 'blur',
      message: t('dataIn.transformer.sepntip')
    }
  ],
  sep: [
    {
      required: true,
      trigger: 'blur',
      message: t('dataIn.transformer.septip')
    }
  ]
});

const splitFormRef = ref();

const emit = defineEmits(['update:ruleForm']);

watch(
  () => transformerState.splitExpresList,
  val => {
    const middleObj = cloneDeep(val) as SplitExpresListType;
    if (middleObj.names && Array.isArray(middleObj.names)) {
      middleObj.names = middleObj.names.toString();
    }
  },
  {
    deep: true
  }
);

watch(localeRuleForm, newData => {
  emit('update:ruleForm', newData);
});

onMounted(() => {
  if (transformerState.splitExpresList) {
    const middleobj = cloneDeep(transformerState.splitExpresList) as SplitExpresListType;
    if (transformerState.splitExpresList.names && Array.isArray(transformerState.splitExpresList.names)) {
      middleobj.names = transformerState.splitExpresList.names.toString();
    }
  }
});

function submit() {
  splitFormRef.value.validate((valid: boolean) => {
    if (valid) {
      isValid.value = true;
      const splitExpre = {} as SplitExpresListType;
      Object.keys(localeRuleForm)
        .filter(key => localeRuleForm[key])
        .forEach(item => {
          splitExpre[item] =
            item == 'names'
              ? localeRuleForm[item].toString()
              : item == 'n'
                ? Number(localeRuleForm[item])
                : localeRuleForm[item].toString().trim();
        });
      if (splitExpre.names) {
        const result = splitExpre.names
          .toString()
          .split(',')
          .map(val => val.trim());
        splitExpre.names = result;
      }
      transformerState.splitExpresList = splitExpre;
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
.split-expression {
  .el-form {
    display: grid;
    grid-template-columns: 1fr 1fr;
    column-gap: 0 !important;

    :deep(.el-input-group__prepend) {
      padding: 0 4px !important;
      border-radius: 0 !important;
    }
  }

  .el-form-item {
    .split-item {
      border: none !important;
    }

    &:not(:last-child) {
      :deep(.el-input__inner) {
        border-right: none !important;
      }
    }
  }
}
</style>
