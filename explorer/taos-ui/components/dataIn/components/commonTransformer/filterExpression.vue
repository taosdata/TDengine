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
import { t } from 'locales';
import { transformerState, supportTransform } from './util';
import { getDataInProps } from 'components/dataIn/model/useDataIn';
import { ElMessage } from 'element-plus';
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
    transformerState.transformResultTable = supportTransform.is_sparkplugb ? result.map((entry: any) => {
      return entry.columns.map((data: any) => {
        return Object.fromEntries(
          entry.fields
            .map((item: { name: any }, index: string | number) => {
              return [
                item.name,
                filterEmpty(data[index])
                  ? Array.isArray(data[index])
                    ? JSON.stringify(data[index])
                    : data[index].toString()
                  : null
              ];
            })
        );
      });
    }).flat(Infinity) : tableData.value;

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
function filterEmpty(val: any) {
  if (Object.is(val, undefined) || Object.is(val, '') || Object.is(val, null)) {
    return '';
  }
  if (Object.is(val, 0) || Object.is(val, false) || Object.is(val, true) || typeof val == 'object') {
    return val.toString();
  }
  return val;
}

//删除filter
function deleteFilter() {
  emit('delete-filter', props.itemData.key);
}

const generateInput:any = inject('generateInput');
//提交
function submitFilter() {
  let parser;
  if (supportTransform.is_sparkplugb) {
    parser = {
      parser: {
        parse: transformerState.topParse?.parser.parse,
        mutate: transformerState.transformExtractParseData
          ? [{ ...transformerState.transformExtractParseData }, { filter: ruleForm.filter_name.trim() }]
          : [{ filter: ruleForm.filter_name.trim() }],
      },
      samples: Array.from(Object.values(generateInput()[0]))
    };
  } else {
    parser = {
      parser: {
        parse: transformerState.topParse?.parser.parse,
        mutate: transformerState.transformExtractParseData
          ? [{ ...transformerState.transformExtractParseData }, { filter: ruleForm.filter_name.trim() }]
          : [{ filter: ruleForm.filter_name.trim() }],
      },
      input: props.datasourceType === 'csv' ? transformerState.csvTransformerParser?.inputList : generateInput()
    };
  }

  transformerState.transformerFilterParseData = {
    filter: ruleForm.filter_name
  };
  isexecuted.value = true;
  getParserData(parser);
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
