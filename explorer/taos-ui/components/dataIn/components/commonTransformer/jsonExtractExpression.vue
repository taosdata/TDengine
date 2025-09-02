<template>
    <div class="json-wrap">
        <el-form ref="jsonFormRef" :model="parseruleForm" :rules="rules" size="default">
            <div class="form-row">
              <span>depth </span>
              <el-input-number
                v-model="parseruleForm.depth"
                style="width: 50px; margin-right: 5px"
                size="default"
                :controls="false"
                :min="0"
              >
              </el-input-number>
              <span>keep</span>
              <el-switch v-model="parseruleForm.keep" size="default">`
              </el-switch>
              <el-input v-model="parseruleForm.expression" style="flex: 1; min-width: 0" :placeholder="t('dataIn.transformer.jsonPlaceholder')"> </el-input>
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
const parseruleForm = reactive(props.ruleForm)
const isValid = ref(true);
const rules = reactive({
    depth: [
        {
            trigger: 'blur',
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
})

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
)

watch(parseruleForm, newData => {
    emit('update:ruleForm', newData);
})

onMounted(() => {
    if (transformerState.jsonExtractListType) {
        const middleobj = cloneDeep(transformerState.jsonExtractListType) as JsonParseExtractType;
        console.log('onMounted:', 'middleObj', middleobj, transformerState.convertExpresList)
    }
})

function submit() {
    jsonFormRef.value.validate((valid: boolean) => {
        if (valid) {
            isValid.value = true;
            const jsonExpr = {} as JsonParseExtractType;
            Object.keys(parseruleForm)
            .filter(key => {
                parseruleForm[key]
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
    })
}

defineExpose({
    submit,
    isValid
});

</script>
<style lang="scss" scoped>
.json-wrap {
    width: 100%;
}

.form-row {
    display: flex;

    > span {
    padding: 0 5px;
    line-height: 30px;
    color: #909399;
    background-color: #f5f7fa;
    border: 1px solid #dcdfe6;
    border-right: 0;
    border-radius: 4px;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }
}
</style>