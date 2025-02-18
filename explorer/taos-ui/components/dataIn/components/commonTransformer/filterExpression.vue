<template>
  <div class="filter-expression">
    <div class="filter-input">
      <el-form ref="filterFormRef" :model="ruleForm" :rules="rules" @submit.prevent>
        <el-form-item prop="filter_name">
          <el-input
            v-model="ruleForm.filter_name"
            size="default"
            :placeholder="t('dataIn.transformer.filter_input')"
            @keyup.enter="excuteFilter"
            @input="changeFilterCont"
          ></el-input>
        </el-form-item>
      </el-form>

      <div class="btns">
        <el-button icon="Delete" @click="deleteFilter"></el-button>
        <el-button @click="excuteFilter">
          <Icon name="PREVIEW" style="width: 16px; height: 16px"></Icon>
        </el-button>
      </div>
    </div>
  </div>
</template>
<script setup lang="ts">
import { getTimeParser } from 'components/dataIn/model/util';
import { t } from 'locales';
import { supportTransform, transformerState } from './util';
import { getDataInProps } from 'components/dataIn/model/useDataIn';
const dataInProps = getDataInProps();

const props = defineProps<{
  itemData: Recordable;
  payload: string;
  // inputparamsColumns: [];
  indentifiedColumns: Recordable[];
  msgForm: Recordable;
  datasourceType: string;
}>();

const isexecuted = ref(false);
const maptypes = ['value', 'generator', 'join', 'format', 'sum', 'expr'];
const ruleForm = reactive({
  filter_name: ''
});
const rules = reactive({
  filter_name: [
    {
      required: false,
      trigger: 'blur',
      message: t('dataIn.transformer.filter_input')
    }
  ]
});
const tableData = ref<any[]>([]);

const filterFormRef = ref();

const emit = defineEmits(['validate-msgbody', 'delete-filter', 'change-filter']);

watch(
  () => props.itemData,
  val => {
    initData(val);
  }
);

onMounted(() => {
  if (props.itemData) {
    initData(props.itemData);
  }
});

function excuteFilter() {
  isexecuted.value = true;
  submit();
}
function changeFilterCont() {
  isexecuted.value = false;
  transformerState.transformerFilterParseData = {
    filter: ruleForm.filter_name
  };
}
function initData(val: Recordable) {
  if (val) {
    ruleForm.filter_name = val.expression;
  }
}
function submit() {
  emit('validate-msgbody');
  if (!props.msgForm.msgbody) {
    return;
  }
  filterFormRef.value?.validate((valid: boolean) => {
    if (valid) {
      submitFilter();
      return true;
    } else {
      return false;
    }
  });
}
async function getParserData(data: any) {
  try {
    const result = await dataInProps.transform.api.getParser(data);
    const tableColumns = result[0].fields.map((item: { name: any }) => item.name);
    if (result.message) {
      ElMessage.error(result.message);
      return;
    }
    emit('change-filter', props.itemData.key, ruleForm.filter_name);
    result[0].columns?.length > 0
      ? (tableData.value = result[0].columns.map((data: { [x: string]: { toString: () => any } }) => {
          return Object.fromEntries(
            result[0].fields.map((item: { name: any }, index: string | number) => {
              return [item.name, data[index] ? data[index].toString() : null];
            })
          );
        }))
      : (tableData.value = [
          Object.fromEntries(
            tableColumns.map((data: any) => {
              return [[data], null];
            })
          )
        ]);
    transformerState.showResultTb = true;
    transformerState.resultTbTitle = 'filterResTb';
    transformerState.transformResultTable = tableData.value;

    const transformerColumns = [
      {
        value: 'expression',
        label: t('expression'),
        children: maptypes.map(item => {
          return {
            value: item,
            label: item
          };
        })
      },
      {
        value: 'mapping',
        label: t('mapping'),
        children: result[0].fields.map((item: { name: any }) => {
          return {
            value: item.name,
            label: item.name
          };
        })
      }
    ];
    transformerState.transformerMapCloumns = transformerColumns;
    transformerState.transResultName = 'filter';
  } catch (error) {
    console.log(error);
  }
}
//删除filter
function deleteFilter() {
  emit('delete-filter', props.itemData.key);
}
//提交
function submitFilter() {
  const resultMsgbody = getResultMsgbody();
  const inputList = getInputList(resultMsgbody);
  const parser = {
    parser: {
      parse: transformerState.topParse?.parser.parse,
      mutate: transformerState.transformExtractParseData
        ? [{ ...transformerState.transformExtractParseData }, { filter: ruleForm.filter_name.trim() }]
        : [{ filter: ruleForm.filter_name.trim() }]
    },
    input:
      props.datasourceType === 'csv'
        ? transformerState.csvTransformerParser?.inputList
        : supportTransform.supportSQL
          ? transformerState.topParse?.input
          : inputList
  };

  transformerState.transformerFilterParseData = {
    filter: ruleForm.filter_name
  };
  isexecuted.value = true;
  getParserData(parser);
}

function getResultMsgbody(): string[] {
  let resultMsgbody = [];
  if (props.msgForm.msgbody.replace(/\}\s*\{/g, '}{').includes('}{')) {
    resultMsgbody = props.msgForm.msgbody.replace(/\}\s*\{/g, '}&${').split('&$');
  } else {
    if (/\n/g.test(props.msgForm.msgbody) && /^[^{]/.test(props.msgForm.msgbody.trim())) {
      //普通文本，目前第一列暂时不能为json格式
      resultMsgbody = props.msgForm.msgbody.replace(/[\n\s]/g, '*&$*').split('*&$*');
    } else {
      try {
        if (/^\{/g.test(props.msgForm.msgbody) && JSON.parse(props.msgForm.msgbody)) {
          resultMsgbody = [].concat(props.msgForm.msgbody);
        }
      } catch (error) {
        ElMessage.error(t('dataIn.transformer.jsontip'));
      }

      resultMsgbody = props.msgForm.msgbody.split(';');
    }
  }
  return resultMsgbody;
}
function getInputList(resultMsgbody: string[]): Recordable[] {
  let inputList = [];

  inputList = resultMsgbody.map(msg => {
    const inputobj: Recordable = {};
    props.indentifiedColumns.forEach((item: Recordable) => {
      if (props.datasourceType == 'mqtt') {
        if (item.name == 'payload') {
          inputobj['payload'] = msg;
        } else {
          inputobj[item.name] = item.type == 'timestamp' ? getTimeParser(new Date()) : item.name;
        }
      } else if (props.datasourceType == 'kafka') {
        if (item.name == 'value') {
          inputobj['value'] = msg;
        } else {
          inputobj[item.name] = item.type == 'timestamp' ? getTimeParser(new Date()) : item.name;
        }
      } else if (props.datasourceType == 'mongodb') {
        if (item.name == 'value') {
          inputobj['value'] = msg;
        } else {
          inputobj[item.name] = item.type == 'timestamp' ? getTimeParser(new Date()) : item.name;
        }
      }
    });
    return inputobj;
  });
  return inputList;
}
</script>
<style lang="scss" scoped>
.filter-expression {
  margin-top: 10px;
  margin-bottom: 20px;
}

.filter-input {
  display: flex;
  align-items: center;
  margin-bottom: 5px;

  .el-form {
    flex: 1;
  }

  .el-form-item {
    margin-bottom: 0 !important;
  }

  .btns {
    display: flex;

    .el-button {
      display: flex;
      align-items: center;
      justify-content: center;
      width: 32px;
      height: 32px;
      padding: 12px 20px;
      border-radius: 6px;

      &:first-child {
        margin-left: 20px;
      }
    }
  }
}

.table {
  margin-bottom: 20px;
}

.tip {
  font-size: 12px;

  .excutetip {
    color: red;

    &.done {
      color: #acaab2;
    }
  }
}
</style>
