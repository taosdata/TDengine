<template>
  <div :class="[
    'extract-split',
    itemData.columnname && itemData.columnname == transformerState.transResultName ? 'active' : ''
  ]">
    <div class="extract-item">
      <el-form ref="extractFormRef" :model="ruleForm" :rules="rules" size="default">
        <el-form-item prop="col_name">
          <el-select v-model="ruleForm.col_name" :placeholder="t('dataIn.transformer.col_select')"
            :disabled="ruleForm.col_name != '' && itemData.columnname != ''" @change="selectCol">
            <el-option v-for="(item, index) in extractColumns" :key="index" :label="item.name" :value="item.name"
              :disabled="!item.show"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="filter_name">
          <el-select v-model="ruleForm.filter_name" :placeholder="t('dataIn.transformer.filter_type')"
            :disabled="isViewable" style="width: 120px; min-width:120px;" @change="changeExtractType">
            <el-option v-for="item in extractTypes" :key="item" :label="item" :value="item"
              :disabled="item == 'join' && itemData.value_type !== 'array'"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="filter_expres">
          <template v-if="ruleForm.filter_name == 'split'">
            <SplitExpression ref="splitExpressionRef" :rule-form="itemData.splitParams" :is-viewable="isViewable">
            </SplitExpression>
          </template>
          <template v-else-if="ruleForm.filter_name == 'convert'">
            <ConvertExpression ref="convertExpressionRef" :rule-form="itemData.convertParams" :is-viewable="isViewable">
            </ConvertExpression>
          </template>
          <el-input v-else v-model="ruleForm.filter_expres"
            :placeholder="t('dataIn.transformer.expre_' + ruleForm.filter_name)" :disabled="isViewable"
            @input="changeExtractExpr"></el-input>
        </el-form-item>
      </el-form>

      <div v-if="!isViewable" class="btns" style="display: flex">
        <el-button icon="Delete" style="display: flex" @click="deleteExtract"></el-button>
        <el-button @click="submit">
          <Icon name="PREVIEW" style="width: 16px; height: 16px"></Icon>
        </el-button>
      </div>
    </div>
    <ul v-if="tableColumns.length > 0 && !isViewable" class="col-list">
      <li v-for="(item, index) in tableColumns.slice(0, 9)" :key="index">
        <span>{{ item.name }}</span>
      </li>
      <li v-if="tableColumns.length > 9">
        <el-tooltip :content="t('dataIn.transformer.viewmore')" placement="top" effect="light">
          <span @click="submit"><i class="More"></i></span>
        </el-tooltip>
      </li>
    </ul>
  </div>
</template>
<script setup lang="ts">
import { getTimeParser } from 'components/dataIn/model/util';
import SplitExpression from './splitExpression.vue';
import ConvertExpression from './convertExpression.vue';
import { cloneDeep } from 'lodash-es';
import { transformerState, supportTransform, defaultColsMap, hiddenColsMap, checkParseData, filterEmpty } from './util';
import { t } from 'locales';
import { TransformExtractParseDataType, TopParseType, SpbTopParseType } from './type';
import { getDataInProps } from 'components/dataIn/model/useDataIn';
const dataInProps = getDataInProps();

const props = defineProps<{
  itemData: Recordable;
  indexKey: number | string;
  extractColumns: Recordable[];
  indentifiedColumns: Recordable[];
  datasourceType: string;
  extractArr: Recordable[];
  msgForm: Recordable;
  isViewable: boolean;
}>();

const isJson = ref<boolean>(true);
const disabled = ref<boolean>(false);
let extractParseData = reactive<TransformExtractParseDataType>({
  extract: {}
});
const tableColumns = ref<Recordable[]>([]);
const maptypes = ['value', 'generator', 'join', 'format', 'sum', 'expr'];
const extractTypes = ['split', 'regex', 'join', 'convert'];
const ruleForm = reactive({
  col_name: '',
  filter_name: '',
  filter_expres: ''
});
const rules = reactive({
  col_name: [
    {
      required: true,
      trigger: 'change',
      message: t('dataIn.transformer.col_select')
    }
  ],
  filter_name: [
    {
      required: true,
      trigger: 'change',
      message: t('dataIn.transformer.filter_type')
    }
  ],
  filter_expres: [
    {
      required: false,
      trigger: 'blur',
      message: t('dataIn.transformer.expre_input')
    }
  ]
});

const tableData = ref<Recordable[]>([]);
const splitExpressionRef = ref();
const convertExpressionRef = ref();
const extractFormRef = ref();

const emit = defineEmits([
  'change-extract-expr',
  'select-column',
  'delete-extract',
  'validate-msgbody',
  'update-extract-arr'
]);

watch(
  () => props.itemData,
  val => {
    initData(val);
  },
  {
    deep: true
  }
);

onMounted(() => {
  if (props.itemData) {
    initData(props.itemData);
    if (props.itemData.columnname && props.itemData.columnname == ruleForm.col_name) {
      disabled.value = true;
    }
  }
});

function changeExtractExpr(val: string) {
  emit('change-extract-expr', ruleForm.col_name, val);
}
function initData(val: Recordable) {
  ruleForm.col_name = val.columnname;
  ruleForm.filter_expres = val.expression;
  ruleForm.filter_name = val.type;
}
function selectCol() {
  disabled.value = true;
  emit('select-column', props.indexKey, ruleForm.col_name);
}
function changeExtractType() {
  const index = props.extractArr.findIndex(item => item.columnname == ruleForm.col_name);
  emit('update-extract-arr', index, ruleForm.filter_name);
}
function submit() {
  emit('validate-msgbody');
  if (!props.msgForm.msgbody) {
    return;
  }
  if (ruleForm.filter_name == 'split') {
    splitExpressionRef.value?.submit();
    if (!splitExpressionRef.value?.isValid) {
      return;
    }
  }
  extractFormRef.value?.validate(async (valid: boolean) => {
    if (valid) {
      transformerState.transResultName = props.itemData.columnname;
      await submitExtract();
      await submitExtract(true);

      return true;
    } else {
      return false;
    }
  });
}
async function getParserData(data: any, isall: boolean | undefined) {
  try {
    const checkResult = checkParseData(data);
    if (checkResult) {
      ElMessage.warning(t(checkResult));
      return;
    }
    const result = await dataInProps.transform.api.getParser(data);
    if (result.message) {
      ElMessage.error(result.message);
      return;
    }

    const mappingObj = {
      value: 'mapping',
      label: t('mapping'),
      children: result[0].fields.map((item: { name: any }) => {
        return {
          value: item.name,
          label: item.name
        };
      })
    };

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
      { ...mappingObj }
    ];

    let colLists = [];
    let tbdata = [];

    colLists = (
      props.datasourceType == 'csv'
        ? result[0].fields
        : result[0].fields.filter((val: { name: string }) => {
          if (props.datasourceType == 'mqtt' && !defaultColsMap.mqtt.includes(val.name)) {
            return val;
          } else if (props.datasourceType == 'kafka' && !defaultColsMap.kafka.includes(val.name)) {
            return val;
          } else if (props.datasourceType == 'mongodb' && !defaultColsMap.mongodb.includes(val.name)) {
            return val;
          } else {
            return val;
          }
        })
    ).map((item: { name: any; type: string }) => {
      return {
        description: item.name,
        name: item.name,
        show: true,
        type: 'string',
        localType: item.type
      };
    });

    tbdata = supportTransform.is_sparkplugb ?
      result.map((result: any) => {
        return result.columns.map((data: { toString: () => any }[]) => {
          return Object.fromEntries(
            result.fields.map((item: { name: any }, index: number) => {
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
      }).flat(Infinity)
      : result[0].columns.map((data: { toString: () => any }[]) => {
        return Object.fromEntries(
          result[0].fields.map((item: { name: any }, index: number) => {
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

    if (isall) {
      transformerColumns.splice(1, 1, mappingObj);
      transformerState.transformerMapCloumns = transformerColumns;
      // 将当前提取或拆分的数据放在 table 的最前面
      const resultData: Recordable[] = [];
      tbdata.map((item: Recordable, index: number) => {
        tableData.value.map(addItem => {
          Object.keys(addItem).forEach(key => {
            delete item[key];
          });
        });
        const addObj = tableData.value[index];
        const newItem = { ...addObj, ...item };
        resultData.push(newItem);
      });
      if (!props.isViewable) {
        transformerState.showResultTb = true;
      }

      transformerState.resultTbTitle = 'extractResTb';
      transformerState.transformResultTable = resultData;
      transformerState.stbDefaultColumns = colLists;

      return;
    }
    tableColumns.value = colLists.map((item: { name: string }) => {
      const obj: Recordable = {};
      const finalVal = tbdata.map((val: Recordable) => val[item.name]);
      obj.name = item.name;
      obj.value = finalVal.join('') ? finalVal.join(' ; ') : '';
      return obj;
    });

    tableData.value = tbdata;
    transformerState.activeColumns = Object.keys(tbdata[0]);
  } catch (error) {
    console.log(error);
  }
}

//提交单个
async function submitExtract(isall?: boolean) {
  const parser = getParserParams(isall);

  await getParserData(parser, isall);
}

function getResultMsgbody(): string[] {
  let resultMsgbody: string[] = [];
  if (props.msgForm.msgbody.replace(/\}\s*\{/g, '}{').includes('}{')) {
    //多json对象
    resultMsgbody = props.msgForm.msgbody
      .replace(/\}\s*\{/g, '}&${')
      .trim()
      .split('&$');
    isJson.value = true;
  } else {
    // 正则过不了语法检查 后续测试
    // if (/\n/g.test(props.msgForm.msgbody) && /^[^\{]/.test(props.msgForm.msgbody.trim())) {
    if (/\n/g.test(props.msgForm.msgbody) && /^[^{]/.test(props.msgForm.msgbody.trim())) {
      //普通文本，目前第一列暂时不能为json格式
      resultMsgbody = props.msgForm.msgbody.replace(/[\n\s]/g, '*&$*').split('*&$*');
      isJson.value = false;
    } else {
      try {
        if (/^\{/g.test(props.msgForm.msgbody) && JSON.parse(props.msgForm.msgbody)) {
          //单json对象
          resultMsgbody = [].concat(props.msgForm.msgbody);
          isJson.value = true;
        }
      } catch (error) {
        ElMessage.error(t('dataIn.transformer.jsontip'));
      }

      resultMsgbody = props.msgForm.msgbody.split(';');
    }
  }
  return resultMsgbody;
}
function getInputList(resultMsgbody: string[], isall?: boolean): Recordable[] {
  let inputList = [];
  let hiddenCols: string[] = [];
  if (!isall) {
    hiddenCols = hiddenColsMap[props.datasourceType] || [];
  } else {
    hiddenCols = [];
  }

  inputList = resultMsgbody.map(msg => {
    const inputobj: Recordable = {};
    props.indentifiedColumns
      .filter((val: Recordable) => !hiddenCols.includes(val.name))
      .forEach((item: Recordable) => {
        if (props.datasourceType == 'mqtt') {
          if (item.name == 'payload') {
            inputobj['payload'] = isall
              ? msg
              : isJson.value
                ? JSON.stringify({
                  [`${props.itemData.columnname}`]: JSON.parse(msg.replace(/\n/g, '\\n'))[props.itemData.columnname]
                })
                : msg;
          } else {
            inputobj[item.name] = item.type == 'timestamp' ? getTimeParser(new Date()) : item.name;
          }
        } else if (props.datasourceType == 'kafka') {
          if (item.name == 'value') {
            inputobj['value'] = isall
              ? msg
              : isJson.value
                ? JSON.stringify({
                  [`${props.itemData.columnname}`]: JSON.parse(msg)[props.itemData.columnname]
                })
                : msg;
          } else {
            inputobj[item.name] = item.type == 'timestamp' ? getTimeParser(new Date()) : item.name;
          }
        } else if (props.datasourceType == 'mongodb') {
          if (item.name == 'value') {
            inputobj['value'] = isall
              ? msg
              : isJson.value
                ? JSON.stringify({
                  [`${props.itemData.columnname}`]: JSON.parse(msg)[props.itemData.columnname]
                })
                : msg;
          } else {
            inputobj[item.name] = item.type == 'timestamp' ? getTimeParser(new Date()) : item.name;
          }
        }
      });
    return inputobj;
  });

  if (props.datasourceType == 'mqtt') {
    inputList = inputList.map((msg: any, index: string | number) => {
      let inputobj = { ...msg };
      inputobj[props.itemData.columnname] =
        props.msgForm.topicbody[index] && props.msgForm.topicbody[index][props.itemData.columnname];
      if (isall) {
        inputobj = { ...props.msgForm.topicbody[index], ...inputobj };
      }
      if (inputobj.payload === '{}') {
        delete inputobj.payload;
      }
      return inputobj;
    });
  }
  return inputList;
}

function getExtractData(): Recordable {
  extractParseData = Object.assign({
    extract: {}
  });
  cloneDeep(props.extractArr)
    .map(item => {
      let value;

      if (item.type === 'regex' || item.type === 'join') {
        value = item.expression;
      } else if (item.type === 'split') {
        // 处理split类型
        const splitobj: Recordable = Object.fromEntries(
          Object.entries(item.splitParams).filter(([value]) => value != null && value !== '')
        );
        splitobj.n = Number(splitobj.n);
        Object.hasOwnProperty.call(splitobj, 'names') ? (splitobj['names'] = splitobj['names'].split(',')) : splitobj;
        value = splitobj;
      } else if (item.type === 'convert') {
        const convertobj: Recordable = Object.fromEntries(
          Object.entries(item.convertParams).filter(([value]) => value != null && value !== '')
        );
        value = {
          convert: typeof convertobj.convert == "string" ? JSON.parse(convertobj.convert) : convertobj.convert,
          new_field_name: convertobj.new_field_name
        };
      } else {
        // 处理其他类型
        value = item.expression ? item.expression.split(';').map((str: string) => str.trim()) : item.expression;
      }

      if (item.type === 'convert') {
        return {
          [`${item.columnname}`]: value
        };
      } else {
        return {
          [`${item.columnname}`]: {
            [`${item.type}`]: value
          }
        };
      }
    })
    .forEach(val => {
      Object.assign(extractParseData['extract'], val);
    });

  const keys = Object.keys(extractParseData.extract);
  const slicedKeys = keys.slice(0, Number(props.indexKey) + 1);
  const slicedObj = slicedKeys.reduce((acc: any, key) => {
    acc[key] = extractParseData.extract[key];
    return acc;
  }, {});

  transformerState.transformExtractParseData = extractParseData;
  return slicedObj;
}
function getParserParams(isall?: boolean): Recordable {
  const resultMsgbody = getResultMsgbody();
  const inputList = getInputList(resultMsgbody, isall);
  const slicedObj = getExtractData();

  if (supportTransform.is_sparkplugb) {
    const topparse = cloneDeep(transformerState.topParse) as SpbTopParseType;
    topparse['parser']['mutate'] = isall
      ? [{ extract: slicedObj }]
      : [
        {
          extract: {
            [`${props.itemData.columnname}`]: extractParseData['extract'][props.itemData.columnname]
          }
        }
      ];
    return {
      parser: {
        parse: topparse.parser.parse,
        mutate: topparse['parser']['mutate']
      },
      samples: topparse.samples
    }
  }

  const topparse = cloneDeep(transformerState.topParse) as TopParseType;

  topparse['parser']['mutate'] = isall
    ? [{ extract: slicedObj }]
    : [
      {
        extract: {
          [`${props.itemData.columnname}`]: extractParseData['extract'][props.itemData.columnname]
        }
      }
    ];

  const parser = {
    parser: {
      parse: topparse.parser.parse,
      mutate: topparse['parser']['mutate']
    },
    input:
      props.datasourceType == 'csv'
        ? isall
          ? transformerState.csvTransformerParser?.inputList
          : transformerState.csvTransformerParser?.inputList.map(item => {
            if (Object.keys(item).includes(props.itemData.columnname)) {
              return {
                [props.itemData.columnname]: item[props.itemData.columnname]
              };
            }
          })
        : supportTransform.supportSQL
          ? isall
            ? topparse.input
            : [
              topparse.input.map((_item, index) => {
                return {
                  [`${props.itemData.columnname}`]: topparse.input[index][props.itemData.columnname]
                };
              })
            ]
          : inputList
  };

  if (!isall) {
    switch (props.datasourceType) {
      case 'mqtt':
        if (parser.parser.parse && parser.parser.parse.payload) {
          parser.parser.parse.payload.json = '';
        }
        break;
      case 'kafka':
      case 'mongodb':
        if (parser.parser.parse && parser.parser.parse.value) {
          parser.parser.parse.value.json = '';
        }
        break;
    }
  }
  return parser;
}
function deleteExtract() {
  emit('delete-extract', props.indexKey, ruleForm.col_name);
}

defineExpose({
  submitExtract
});
</script>
<style lang="scss" scoped>
@keyframes heart {
  0% {
    box-shadow: 0 0 5px #4259ce;
  }

  // 50%{
  //   box-shadow: 0 0 20px #4259ce;
  // }
  100% {
    box-shadow: 0 0 5px #4259ce;
  }
}

.extract-split {
  // &.active{
  //   padding: 20px;
  //   border-radius:6px;
  //   animation:heart 5s linear infinite;
  // }
  margin-bottom: 12px;

  .extract-item {
    display: flex;
    flex-wrap: nowrap;

    .el-form {
      display: flex;
      flex: 1;
      column-gap: 15px;

      & div:last-of-type {
        flex: 1;
      }
    }

    .el-input:first-child {
      margin-left: 0;
    }

    .btns {
      display: flex;
      flex-wrap: nowrap;

      .el-button {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 32px;
        height: 32px;
        padding: 12px 20px;
        border-radius: 6px;

        /* border: 1px solid #4259ce; */
        &:first-child {
          margin-left: 20px;
        }
      }
    }
  }
}

.table {
  max-height: 300px;
  overflow-y: auto;
}

.el-form-item--small.el-form-item {
  margin-bottom: 10px;
}

.col-list {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  gap: 15px;
  max-height: 80px;
  margin-bottom: 25px;
  overflow-y: hidden;

  li {
    color: #4259ce;
    text-align: center;
    background: #ecf2fe;
    border: 1px solid #f6f8fa;
    border-radius: 14px;
  }
}
</style>
