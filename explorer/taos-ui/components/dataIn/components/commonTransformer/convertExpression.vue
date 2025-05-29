<template>
  <div class="convert-expression">
    <el-form ref="convertFormRef" :model="localeRuleForm" :rules="rules" size="default">
      <el-form-item prop="convert">
        <el-input v-model="localeRuleForm.convert" placeholder="" class="convert-item" :disabled="isViewable">
          <template #prepend>rule</template>
        </el-input>
      </el-form-item>
      <el-form-item prop="new_field_name">
        <el-input v-model="localeRuleForm.new_field_name" placeholder="" class="convert-item" :disabled="isViewable">
          <template #prepend>name</template>
        </el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script setup lang="ts">
import { cloneDeep } from 'lodash-es';
import { transformerState } from './util';
import { ConvertExpresListType } from './type';
import { t } from 'locales';

const props = withDefaults(
  defineProps<{
    ruleForm: Recordable;
    isViewable: boolean;
  }>(),
  {
    ruleForm: () => {
      return {
        convert: '',
        new_field_name: ''
      };
    }
  }
);
const localeRuleForm = reactive(props.ruleForm);
const isValid = ref(true);
const rules = reactive({
  new_field_name: [
    {
      trigger: 'blur',
      message: t('dataIn.transformer.convnametip')
    }
  ],
  convert: [
    {
      required: true,
      trigger: 'blur',
      message: t('dataIn.transformer.convtip')
    }
  ]
});

const convertFormRef = ref();

const emit = defineEmits(['update:ruleForm']);

watch(
  () => transformerState.convertExpresList,
  val => {
    const middleObj = cloneDeep(val) as ConvertExpresListType;
    console.log('watch:', 'middleObj', middleObj, transformerState.convertExpresList);
  },
  {
    deep: true
  }
);

watch(localeRuleForm, newData => {
  emit('update:ruleForm', newData);
});

onMounted(() => {
  if (transformerState.convertExpresList) {
    const middleobj = cloneDeep(transformerState.convertExpresList) as ConvertExpresListType;
    console.log('onMounted:', 'middleObj', middleobj, transformerState.convertExpresList)
  }
});

function submit() {
  convertFormRef.value.validate((valid: boolean) => {
    if (valid) {
      isValid.value = true;
      const convertExpr = {} as ConvertExpresListType;
      Object.keys(localeRuleForm)
        .filter(key => localeRuleForm[key])
        .forEach(item => {
          convertExpr[item] = localeRuleForm[item].toString().trim();
        });
     
      transformerState.convertExpresList = convertExpr;
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
.convert-expression {
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
    .convert-item {
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
