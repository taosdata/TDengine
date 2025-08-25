<template>
  <div class="common-transformer">
    <section class="msg-sec">
      <div v-if="supportTransform.supportSQL" class="block-title">
        <span>{{ t('dataIn.transformer.msgbody') }}</span>
      </div>
      <el-row class="mt10">
        <el-col :span="sourceForm.type == 'csv' ? 24 : 17">
          <el-form ref="msgFormRef" :model="msgForm" @submit.prevent>
            <el-form-item prop="msgbody">
              <el-input
                v-model="msgForm.msgbody"
                :disabled="!!supportTransform.supportSQL"
                class="msgbody"
                :placeholder="t('dataIn.transformer.msgbodytip')"
                size="default"
                type="textarea"
                :autosize="{ minRows: 7, maxRows: 7 }"
              ></el-input>
            </el-form-item>
          </el-form>
        </el-col>
        <el-col v-if="sourceForm.type !== 'csv'" :span="7" style="padding-left: 8px">
          <div class="flex-between">
            <span style="display: inline-block; width: 126px">{{ t('dataIn.transformer.dataLimit') }}</span>
            <el-input-number
              v-model="transformerState.limitOffset"
              class="flex-1"
              size="default"
              :min="1"
              :max="100"
              controls-position="right"
            ></el-input-number>
          </div>
          <div v-if="supportTransform.supportSQL" :class="['flex-between', 'mt5']">
            <span style="display: inline-block; width: 126px">{{ t('dataIn.transformer.timeout') }}</span>
            <el-input-number
              v-model="timeout"
              class="flex-1"
              size="default"
              :min="1"
              :max="600"
              controls-position="right"
            ></el-input-number>
          </div>
          <el-col name="second" :class="['mt5', 'msg-right']">
            <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
              <template #content>
                <span v-dompurify-html="t('common.communityTip')"></span>
              </template>
              <el-button
                type="primary"
                plain
                size="default"
                :loading="requesting"
                :disabled="dataInProps.isCommunity"
                @click="getMsgBody"
                >{{ t('dataIn.transformer.msgbodytypes.retrieve') }}</el-button
              >
            </el-tooltip>
          </el-col>
          <el-col v-if="!supportTransform.supportSQL" name="third" :class="['mt5', 'msg-right']">
            <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
              <template #content>
                <span v-dompurify-html="t('common.communityTip')"></span>
              </template>
              <el-upload
                class="upload-demo"
                :action="dataInProps.uploadFileUrl"
                :data="{ req_id: 'taosx-demo-file' }"
                :before-remove="beforeRemove"
                :on-success="handleSuccess"
                :on-progress="handleStart"
                :on-error="handleError"
                :on-exceed="handleExceed"
                :file-list="fileList"
                :show-file-list="false"
              >
                <el-button size="default" type="primary" plain :loading="request" :disabled="dataInProps.isCommunity">{{
                  t('dataIn.transformer.msgbodytypes.type3')
                }}</el-button>
              </el-upload>
            </el-tooltip>
          </el-col>
          <el-col v-if="!supportTransform.supportSQL" name="first" :class="['mt5', 'msg-right']">
            <el-button size="default" @click="clearMsgBody">{{ t('dataIn.transformer.msgbodytypes.type1') }}</el-button>
          </el-col>
        </el-col>
      </el-row>
    </section>
    <section class="extract">
      <div class="block-title top">
        <span>{{
          sourceForm.type == 'csv' || supportTransform.supportSQL
            ? t('dataIn.transformer.identified')
            : t('dataIn.transformer.parse')
        }}</span>
        <el-popover placement="top" effect="light" trigger="hover" width="520">
          <div style="position: relative">
            <i style="position: absolute; right: 0" class="el-icon-close"></i>
            <DocsContent :style="docsStyle" :content="t('dataIn.transformer.extractdesc')" />
          </div>
          <template #reference>
            <span v-if="!supportTransform.supportSQL && sourceForm.type !== 'csv'" style="margin-left: 1px">
              <Icon name="label_info" class="info-icon-custom"></Icon>
            </span>
          </template>
        </el-popover>
      </div>
      <div v-if="sourceForm.type !== 'csv' && !supportTransform.supportSQL" class="extrac-parse">
        <el-form :rules="parseRules" :model="parseruleForm">
          <el-form-item prop="type">
            <el-select
              v-model="parseruleForm.type"
              size="default"
              :placeholder="t('dataIn.transformer.filter_type')"
              @change="handleTypeChange"
            >
              <el-option v-for="item in parseTypes" :key="item" :label="item" :value="item"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item prop="expression">
            <el-input
              v-if="parseruleForm.type == 'regex'"
              v-model="parseruleForm.expression"
              :placeholder="'(?<y>[0-9]{4})-(?<m>[0-9]{2})-(?<d>[0-9]{2})'"
              size="default"
            >
            </el-input>
            <div v-else-if="parseruleForm.type == 'json'" class="josn-wrap">
              <span>depth </span>
              <el-input-number
                v-model="parseruleForm.depth"
                style="width: 100px; margin-right: 5px"
                size="default"
                :controls="false"
                :min="0"
              >
              </el-input-number>
              <span>keep</span>
              <el-switch v-model="parseruleForm.keep" style="width: 100px; margin-left: 5px" size="default">
              </el-switch>
              <CusSelect
                v-model="parseruleForm.expression"
                :all-properties="allProperties"
                :depth="parseruleForm.depth"
                :keep="parseruleForm.keep"
                @select-json="selectJson"
                @update-data="updateData"
              />
            </div>
            <div v-else-if="parseruleForm.type == 'udt'" style="display: inline-flex; align-items: start; width: 100%">
              <el-input
                v-model="parseruleForm.expression"
                size="default"
                type="textarea"
                :autosize="{ minRows: 1, maxRows: 7 }"
              ></el-input>
              <el-upload
                size="default"
                style="margin-left: 10px"
                :action="dataInProps.uploadFileUrl"
                :data="uploadData"
                :before-remove="beforeRemove"
                :on-success="handleSuccessUdt"
                :on-error="handleError"
                :file-list="fileList"
                :show-file-list="false"
              >
                <el-button
                  size="default"
                  plain
                  type="primary"
                  style="width: auto; padding: 0 6px; margin-top: 0"
                  :disabled="dataInProps.isCommunity"
                >
                  {{ t('dataIn.transformer.uploadCode') }}
                </el-button>
              </el-upload>
            </div>
            <el-input v-else v-model="parseruleForm.expression" size="default"> </el-input>
          </el-form-item>
          <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
            <template #content>
              <span v-dompurify-html="t('common.communityTip')"></span>
            </template>
            <el-button
              size="default"
              style="display: flex"
              :disabled="
                msgForm.msgbody == '' ||
                ((parseruleForm.type == 'udt' || parseruleForm.type == 'regex') && parseruleForm.expression == '') ||
                dataInProps.isCommunity
              "
              @click="submitParse"
            >
              <Icon name="PREVIEW" style="width: 16px; height: 16px"></Icon>
            </el-button>
          </el-tooltip>
        </el-form>
      </div>
    </section>
    <section v-if="columnsArr.length > 0">
      <ul :class="['col-list', transformerState.transResultName == t('dataIn.transformer.identified') ? 'active' : '']">
        <template v-for="(item, index) in columnsArr">
          <li v-if="index < 9" :key="index">
            <span>{{ item.name }}</span>
          </li>
        </template>
        <li v-if="columnsArr.length > 9">
          <el-tooltip :content="t('dataIn.transformer.viewmore')" placement="top" effect="light"
            ><span @click="submitParse"><i class="el-icon-more"></i></span>
          </el-tooltip>
        </li>
      </ul>
    </section>
    <section class="extract">
      <div class="block-title top">
        <span>{{ t('dataIn.transformer.extract') }}</span>
        <el-popover placement="top" trigger="hover" width="520">
          <div style="position: relative">
            <i style="position: absolute; right: 0" class="el-icon-close"></i>
            <DocsContent :style="docsStyle" :content="t('dataIn.transformer.subextractdesc')" />
          </div>
          <template #reference>
            <span style="margin-left: 1px">
              <Icon name="label_info" class="info-icon-custom"></Icon>
            </span>
          </template>
        </el-popover>
      </div>
      <template v-for="(item, index) in extractArr" :key="item.key">
        <ExtractSplit
          ref="extractRef"
          :item-data="item"
          :index-key="index"
          :datasource-type="sourceForm.type"
          :msg-form="msgForm"
          :extract-arr="extractArr"
          :extract-columns="item.columns"
          :indentified-columns="indentifiedColumns"
          :is-viewable="isViewable"
          @update-extract-arr="updateExtractArr"
          @validate-msgbody="validateMsgBody"
          @delete-extract="deleteExtract"
          @select-column="changeColumnStatus"
          @set-extract-name="setExtractName"
          @change-extract-expr="changeExtractExpr"
        ></ExtractSplit>
      </template>
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
        <template #content>
          <span v-dompurify-html="t('common.communityTip')"></span>
        </template>
        <el-button
          type="primary"
          icon="el-icon-plus"
          size="default"
          class="btn-icon-small"
          plain
          :disabled="columnsArr.length == 0 || dataInProps.isCommunity"
          @click="addNewExtract"
        >
          {{ t('dataIn.transformer.addExtract') }}
        </el-button>
      </el-tooltip>
    </section>
    <section class="filter">
      <div class="block-title">
        <span>{{ t('dataIn.transformer.filter') }}</span>
        <el-popover placement="top" effect="light" trigger="hover" width="520">
          <div style="position: relative">
            <i style="position: absolute; right: 0" class="el-icon-close"></i>
            <DocsContent :style="docsStyle" :content="t('dataIn.transformer.filterdesc')" />
          </div>
          <template #reference>
            <span style="margin-left: 1px">
              <Icon name="label_info" class="info-icon-custom"></Icon>
            </span>
          </template>
        </el-popover>
      </div>
      <template v-for="(item, index) in filterArr" :key="index">
        <FilterExpression
          ref="filterRef"
          :index="index"
          :item-data="item"
          :payload="msgForm.msgbody"
          :msg-form="msgForm"
          :datasource-type="sourceForm.type"
          :indentified-columns="indentifiedColumns"
          @validate-msgbody="validateMsgBody"
          @delete-filter="deleteFilter"
          @change-filter="changeFilter"
        >
        </FilterExpression>
      </template>
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
        <template #content>
          <span v-dompurify-html="t('common.communityTip')"></span>
        </template>
        <el-button
          type="primary"
          icon="el-icon-plus"
          size="default"
          class="btn-icon-small"
          plain
          :disabled="filterArr.length >= 1 || columnsArr.length == 0 || dataInProps.isCommunity"
          @click="addNewFilter"
        >
          {{ t('dataIn.transformer.addfilter') }}
        </el-button>
      </el-tooltip>
    </section>
    <section>
      <div class="block-title">
        <span>{{ t('dataIn.transformer.superconfig') }}</span>
      </div>
      <div class="table-content">
        <div class="table-title" style="margin-bottom: 16px">
          <div class="title">
            <span style="color: #4259ce">
              {{ t('dataIn.transformer.targetSt') }}
            </span>
            <el-form ref="sruleFormRef" :model="sruleForm" :rules="srules">
              <el-form-item prop="s_name">
                <el-select
                  v-model="sruleForm.s_name"
                  allow-create
                  default-first-option
                  size="default"
                  :placeholder="
                    sourceForm.targetDB
                      ? t('dataIn.transformer.stableSelectOrCreateTip')
                      : t('dataIn.transformer.databaseSelectTip')
                  "
                  :disabled="!sourceForm.targetDB || columnsArr.length === 0"
                  @change="() => getSTbaleList(false)"
                >
                  <el-option v-for="(item, index) in stableLists" :key="index" :label="item" :value="item"></el-option>
                </el-select>
              </el-form-item>
            </el-form>
          </div>
          <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
            <template #content>
              <span v-dompurify-html="t('common.communityTip')"></span>
            </template>
            <el-dropdown v-if="supportTransform.supportTopicBody" size="default" @command="createStable">
              <el-button size="default" type="primary" plain>
                {{ t('dataIn.transformer.createstb') }}
                <el-icon class="el-icon--right"><arrow-down /></el-icon>
              </el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="sqlCreate">{{ t('dataIn.transformer.createstb') }}</el-dropdown-item>
                  <el-dropdown-item command="templateCreate">{{
                    t('dataIn.transformer.templatestb')
                  }}</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
            <el-button
              v-else
              type="primary"
              class="btn-icon-small"
              size="default"
              icon="Plus"
              plain
              :disabled="sourceForm.targetDB == '' || columnsArr.length === 0 || dataInProps.isCommunity"
              @click="createStable('sqlCreate')"
            >
              {{ t('dataIn.transformer.createstb') }}
            </el-button>
          </el-tooltip>
        </div>
        <div v-if="tableData.length > 0" :key="refreshKey" class="table-detail">
          <el-table :data="pageTableData" border style="width: 100%">
            <el-table-column prop="Name" show-overflow-tooltip label="Name" width="180px">
              <template #default="scope">
                <div style="display: flex; align-items: end">
                  <el-icon v-if="scope.row.Expression.toString()" style="margin-right: 2px; color: rgb(56 155 255)">
                    <SuccessFilled />
                  </el-icon>
                  <Icon
                    v-if="params_tags.includes(scope.row['Name'])"
                    :name="'tag'"
                    class="console-tree-icon"
                    style="width: 20px; height: 20px"
                  ></Icon>
                  <Icon
                    v-if="scope.row.PrimaryKey"
                    :name="'key'"
                    class="console-tree-icon"
                    style="width: 20px; height: 20px"
                  ></Icon>

                  <span>{{ scope.row['Name'] }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="Type" show-overflow-tooltip label="Type" width="150px"></el-table-column>
            <el-table-column prop="Expression" label="Expression">
              <template #header>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :style="docsStyle" :content="t('dataIn.transformer.expressiondesc')" />
                  </template>
                  <span>Expression <Icon name="label_info" class="info-icon-custom"></Icon></span>
                </el-tooltip>
              </template>
              <template #default="scope">
                <div class="box-expression">
                  <template v-if="scope.row['Name'] == 'SubTableName'">
                    <el-input v-model="scope.row.Expression" size="default" :placeholder="exprformat"></el-input>
                  </template>
                  <template v-else>
                    <el-select
                      v-model="scope.row.exprname"
                      size="default"
                      class="mapping-rule-select"
                      style="width: 110px; min-width: 110px"
                      @change="changeCurrentMapExpr(scope)"
                    >
                      <el-option v-for="item in mappingTypes" :key="item" :label="item" :value="item">{{
                        item
                      }}</el-option>
                    </el-select>

                    <el-select
                      v-if="
                        scope.row.exprname == 'mapping' || scope.row.exprname == 'sum' || scope.row.exprname == 'join'
                      "
                      :key="Math.random()"
                      v-model="scope.row.Expression"
                      :placeholder="t('dataIn.transformer.coltip')"
                      :clearable="scope.row.exprname == 'mapping'"
                      size="default"
                      filterable
                      class="mapping-rule-expression"
                      :multiple="scope.row.exprname != 'mapping'"
                    >
                      <el-option
                        v-for="val in mappingcolumns"
                        :key="val.label"
                        :value="val.value"
                        :label="val.label"
                      ></el-option>
                    </el-select>
                    <el-input
                      v-else
                      :key="'expr'"
                      v-model="scope.row.Expression"
                      class="mapping-rule-expression"
                      :placeholder="
                        scope.row.exprname == 'format'
                          ? exprformat
                          : scope.row.exprname == 'expr'
                            ? exprexpression
                            : scope.row.exprname == 'value'
                              ? t('dataIn.transformer.valuetip')
                              : ''
                      "
                      size="default"
                      :disabled="scope.row['exprname'] == 'generator'"
                      @change="statisticCol"
                    ></el-input>
                    <!-- 第三列组件 -->
                    <el-input
                      v-if="scope.row.exprname == 'join'"
                      :key="'exprjoin'"
                      v-model="scope.row.joinwith"
                      size="default"
                      class="mapping-rule-extra"
                      style="height: 32px"
                    >
                      <template #prepend>with</template>
                    </el-input>
                    <el-input
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataRange"
                      :key="'default-value-of-' + scope.row['Name']"
                      v-model="scope.row.default"
                      size="default"
                      type="number"
                      :placeholder="t('dataIn.transformer.defaultValuePlaceholder')"
                      :maxlength="scope.row.dataRange[2]"
                      class="mapping-rule-extra"
                      @blur="onDefaultValueInput(scope.row.Name, scope.row.default, scope.row.dataRange)"
                    >
                    </el-input>
                    <el-select
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataType == 'BOOL'"
                      v-model="scope.row.default"
                      :placeholder="t('dataIn.transformer.defaultValuePlaceholder')"
                      size="default"
                      class="mapping-rule-extra"
                    >
                      <el-option label="true" value="true"></el-option>
                      <el-option label="false" value="false"></el-option>
                      <el-option label="null" value="null"></el-option>
                    </el-select>
                    <el-input
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataType"
                      v-model="scope.row.default"
                      size="default"
                      :placeholder="t('dataIn.transformer.defaultValuePlaceholder')"
                      class="mapping-rule-extra"
                    ></el-input>
                  </template>
                  <div v-if="scope.row.defaultValueError" class="default-value-error">
                    {{ scope.row.defaultValueError }}
                  </div>
                </div>
              </template>
            </el-table-column>
          </el-table>
          <div class="block-page">
            <el-pagination
              :class="['pagination', pageCount < 20 ? 'hide' : '']"
              :page-size="pageSize"
              layout="total,prev, pager, next, jumper,slot"
              :total="pageCount"
              @current-change="handleCurrentChange"
            >
              <div key="1">
                <span style="margin-left: 6px; font-weight: 400; color: #16191f">
                  {{ t('dataIn.transformer.configuredcount') }}
                  {{ configuredCount }}
                  {{ t('dataIn.transformer.unit') }}</span
                >
              </div>
            </el-pagination>

            <el-button size="default" @click="caculateMappingResult">
              <Icon name="PREVIEW" style="width: 16px; height: 16px"></Icon>
            </el-button>
          </div>
        </div>
      </div>
    </section>
    <el-dialog
      v-model="showCreateDialog"
      :title="t('dataIn.transformer.create_st')"
      width="1000px"
      center
      :close-on-click-modal="false"
      @close="closeDialog"
    >
      <CreateStable
        ref="createStbRef"
        :key="componentKey"
        :active-type="activeType"
        :database="sourceForm.targetDB"
        @close="closeDialog"
        @create-stable-succ="createStableSucc"
        @create-template-stable-succ="createTemplateStableSucc"
      >
      </CreateStable>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import type { ComponentInternalInstance } from 'vue';
import ExtractSplit from './extractSplit.vue';
import FilterExpression from './filterExpression.vue';
import DocsContent from 'components/MdRender.vue';
import CusSelect from './cusSelect.vue';
import CreateStable from './createSTB.vue';
import { getDataInProps } from '../../model/useDataIn.js';
import { t } from 'locales';
import {
  convert,
  supportTransform,
  transformerState,
  extractAllProperties,
  getExampleList,
  validateJsonKeys,
  checkParseData,
  defaultColsMap,
  filterEmpty
} from './util.js';
import { currentPageType, sourceForm, getDataRange, getWriteConfigData } from '../../model/util.js';
import { ElMessage, FormInstance } from 'element-plus';
import { executeSqlFn } from 'components/api';
import { isEn } from 'config';
import { isEmpty } from 'lodash-es';
import {
  SpbTopParseType,
  TableRow,
  TopParseType,
  TransformerfullparamsType,
  TransformerSpbfullparamsType,
  TransformExtractParseDataType
} from './type';

const PARSER_BUILDIN = ['json', 'regex', 'udt'];
type ParserBuildinType = (typeof PARSER_BUILDIN)[number] | 'hebeipower' | 'split';
const dataInProps = getDataInProps();

const props = defineProps<{
  parserColumns: Record<string, any>[];
}>();
const sourceParent = inject<ComponentInternalInstance>('sourceParent') as any;
const exprformat = '${c1}-${c2}:${c3}';
const exprexpression = 'centigrade * 1.8 + 32';
const parseTypes = ref<string[]>([]);
interface parseruleFormProp {
  type: ParserBuildinType;
  expression: string;
  depth: undefined;
  keep: boolean;
}
const parseruleForm = reactive<parseruleFormProp>({
  type: 'json',
  expression: '',
  depth: undefined,
  keep: false
});
const configuredCount = ref(0);
const parseRules = reactive({
  type: [
    {
      required: true,
      trigger: 'change',
      message: t('dataIn.transformer.filter_type')
    }
  ]
});
const maptypes = ['value', 'generator', 'join', 'format', 'sum', 'expr'];
const pageSize = ref(20);
const pageCount = ref(10);
const currentPage = ref(1);
const isbreak = ref<boolean>(false); //tranformer创建是否出错
const isCSV = ref<boolean>(false);
const options = ref<any[]>([]);
const mappingcolumns = ref<Recordable[]>([]);
const msgFormRef = ref<FormInstance>();
const msgForm = reactive({
  msgbody: '',
  topicbody: [] as Recordable[]
});
const params_columns = ref<string[]>([]);
const params_tags = ref<string[]>([]);

const extractAddStatus = ref(false);
const mappingTypes = ['mapping', 'value', 'generator', 'join', 'format', 'sum', 'expr'];

const dialogForm = reactive({
  st_name: ''
});

const showCreateDialog = ref<boolean>(false);
const stableLists = ref<string[]>([]);
const sruleFormRef = ref<FormInstance>();
const sruleForm = reactive({
  s_name: ''
});
const uploadData = reactive({
  req_id: new Date().getTime()
});

const fileList = ref([]);
// parseIndetntifiedCols: [],
const indentifiedColumns = ref<Recordable[]>([]);
const columnsArr = ref<Record<string, any>[]>([]);
const tableData = ref<TableRow[]>([]);
const pageTableData = ref<any[]>([]);
const extractArr = ref<Recordable[]>([]);
const filterArr = ref<Recordable[]>([]);
let mappingParser = reactive<Recordable>({});
const timeout = ref(30);
const request = ref(false);
const allProperties = ref<Recordable[]>([]);
const requesting = ref<boolean>(false);
const refreshKey = ref<number>(0);
const componentKey = ref<number>(0);
const activeType = ref<string>('');
const createStbRef = ref();
const extractRef = ref();
const filterRef = ref();

const srules = computed(() => {
  return {
    s_name: [
      {
        required: true,
        trigger: 'change',
        message: t('dataIn.transformer.st_input')
      }
    ]
  };
});
const docsStyle = computed(() => {
  return {
    paddingRight: '20px',
    wordBreak: 'break-word'
  };
});

const isEditable = computed(() => currentPageType.value === 'edit' || currentPageType.value === 'copy');
const isViewable = computed(() => currentPageType.value === 'view');

watch(
  tableData,
  () => {
    statisticCol();
  },
  {
    deep: true
  }
);

watch(isEn, () => {
  refreshKey.value += 1;
  nextTick(() => {
    sruleFormRef.value?.clearValidate();
  });
});

//csv需要单独处理
watch(
  () => transformerState.csvTransformerParser,
  (val: Recordable) => {
    if (val) {
      isCSV.value = true;
      msgForm.msgbody = val.msgBody;
      formatCSVExtract(val?.columns);
    }
  },
  {
    deep: true
  }
);

watch(
  () => transformerState.transformerMapCloumns,
  val => {
    options.value = val;
    mappingcolumns.value = val.filter(item => item.value == 'mapping')[0].children;
    const newmappings = mappingcolumns.value.map(item => item.label);
    tableData.value.map(item => {
      if (item.exprname == 'mapping' && item['Type'] != 'Tablename') {
        if (!newmappings.includes(item['Expression'])) {
          item['Expression'] = '';
        }
        return item;
      }
    });
  },
  {
    deep: true
  }
);

watch(
  () => sourceForm.targetDB,
  () => {
    getInitStables();
  }
);

watch(
  props.parserColumns,
  val => {
    if (sourceForm.type == 'mqtt' || sourceForm.type == 'kafka' || sourceForm.type == 'mongodb') {
      initColumnLists(val.filter(item => item.name != 'ts'));
    } else {
      initColumnLists(val);
    }
  },
  {
    deep: true
  }
);

onBeforeMount(async () => {
  const plugins = await dataInProps.transform.api.listParserPlugins();
  parseTypes.value = PARSER_BUILDIN.concat(plugins.map(item => item.name));
});

onMounted(async () => {
  if (props.parserColumns) {
    if (sourceForm.type == 'mqtt' || sourceForm.type == 'kafka' || sourceForm.type == 'mongodb') {
      initColumnLists(props.parserColumns.filter(item => item.name != 'ts'));
    } else {
      initColumnLists(props.parserColumns);
    }
  }

  if (isEditable.value || (transformerState.csvParser && Object.hasOwn(transformerState.csvParser, 'parser'))) {
    // 编辑状态
    if (transformerState.transformerParserData) {
      await echoParser(transformerState.transformerParserData);
    }
  }
  if (transformerState.csvTransformerParser) {
    //CSV新增
    isCSV.value = true;
    msgForm.msgbody = transformerState.csvTransformerParser.msgBody;
    await submitParse();
  }
  if (sourceForm.type == 'avevaHistorian') {
    timeout.value = 120;
  }
  await getInitStables();
  statisticCol();
});

function statisticCol() {
  configuredCount.value = tableData.value.filter(item => item['Expression'] != '').length;
}
function changeCurrentMapExpr(scope: Recordable) {
  nextTick(() => {
    pageTableData.value[scope.$index].Expression = '';
    if (pageTableData.value[scope.$index].default != undefined && pageTableData.value[scope.$index].default !== '') {
      pageTableData.value[scope.$index].default = '';
      pageTableData.value[scope.$index].defaultValueError = '';
    }
    if (scope.row.exprname == 'generator') {
      pageTableData.value[scope.$index].Expression = 'now';
    }
  });
}
async function getMsgBody() {
  sruleFormRef.value?.clearValidate();
  sourceParent?.refs.formRef.validate(async (valid: boolean) => {
    if (valid) {
      await onValid();
    } else {
      onInvalid();
    }
  });
  async function onValid() {
    requesting.value = true;
    const isSupportType = sourceForm.type == 'kafka' || sourceForm.type == 'mqtt' || sourceForm.type == 'mongodb';
    const params: Recordable = { dsn: sourceForm };
    params.dsn.sample_data_limit = transformerState.limitOffset;
    // if (isSupportType) {
    //   params.dsn.get_sample_timeout = 3;
    // }
    const result = await dataInProps.transform.api.getSampleDataMsgbody(params);
    if (result && Object.hasOwnProperty.call(result, 'code')) {
      ElMessage.error(result.message || result.desc);
      if (!isSupportType) {
        msgForm.msgbody = '';
      }
      requesting.value = false;
      return;
    }
    if (isSupportType) {
      if (result.input.length <= 0) {
        ElMessage.warning(t('dataIn.transformer.retrieveTip'));
      } else {
        let type = '';
        if (sourceForm.type == 'kafka') {
          type = 'Kafka';
        } else if (sourceForm.type == 'mqtt') {
          type = 'MQTT';
        } else if (sourceForm.type == 'mongodb') {
          type = 'MongoDB';
        }
        ElMessage.success(type + t('dataIn.transformer.retrieveSuccTip', [result.input.length]));
      }
      result.input.map((item: Recordable) => {
        msgForm.msgbody += item.payload + '\n';
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { payload, ...rest } = item;
        msgForm.topicbody.push(rest);
      });
    } else {
      msgForm.msgbody = JSON.stringify(result);
    }
    requesting.value = false;
    // mqtt、kafka、mongodb 的从服务器获取数据后，只是追加到示例数据 textarea 中，不触发预览数据
    if (!isSupportType) {
      await submitParse();
    }
  }
  function onInvalid() {
    nextTick(() => {
      document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
    });
  }
}
function clearMsgBody() {
  msgForm.msgbody = '';
  msgForm.topicbody = [];
}

function showIndentifyResulttb() {
  transformerState.showResultTb = true;
  transformerState.resultTbTitle = 'parseResTb';
  if (sourceForm.type == 'csv') {
    nextTick(() => {
      if (document.querySelector('.block-title.top')) {
        const dom = document.querySelector('.block-title.top') as HTMLElement;
        const mainDom = document.querySelector('.main-content') as HTMLElement;
        const top = dom.offsetTop + mainDom.scrollHeight;
        transformerState.transformTableHeight = top;
      }
    });
  }

  transformerState.transResultName = t('dataIn.transformer.identified');
}
function handleExceed(files: string | any[], fileList: string | any[]) {
  ElMessage.warning(
    `当前限制选择 3 个文件，本次选择了 ${files.length} 个文件，共选择了 ${files.length + fileList.length} 个文件`
  );
}
function handleStart() {
  request.value = true;
}
function handleError() {
  request.value = false;
}
function handleSuccess(_: any, file: { raw: Blob }) {
  const reader = new FileReader();

  reader.onload = e => {
    const contents = e.target?.result;
    msgForm.msgbody += contents + '\n';
    request.value = false;
  };

  reader.readAsText(file.raw); // 读取文本文件
}
function handleSuccessUdt(_: any, file: { raw: Blob }) {
  const reader = new FileReader();
  parseruleForm.expression = '';

  reader.onload = e => {
    const contents = e.target?.result;
    parseruleForm.expression += contents + '\n';
    request.value = false;
  };

  reader.readAsText(file.raw); // 读取文本文件
}
function beforeRemove(file: { name: string }) {
  return ElMessageBox.confirm(`确定移除 ${file.name}？`);
}

async function submitParse() {
  try {
    for (let i = 0; i < pageTableData.value.length; i++) {
      if (pageTableData.value[i].defaultValueError) {
        ElMessage.error(pageTableData.value[i].defaultValueError);
        return;
      }
    }

    if (!msgForm.msgbody) {
      ElMessage.warning(t('dataIn.transformer.msgbodytip'));
      return;
    }

    const topParser = getTopParserData();
    await handleParseResult(topParser);
    // 删除 extractArr 中没有包含 columnsArr 中拆分的字段
    handelExtractArr(columnsArr.value, extractArr.value);
    showIndentifyResulttb();
  } catch (error: any) {
    console.log(error);
    ElMessage.error(error?.message);
  }
}
function getTopParserData() {
  let topParser = null;

  if (supportTransform.supportSQL || supportTransform.is_sparkplugb) {
    topParser = JSON.parse(msgForm.msgbody);
  } else {
    let depthObj = {};
    let expressionObj = {};
    let keepObj = {};

    switch (parseruleForm.type) {
      case 'split':
        expressionObj = {
          [parseruleForm.type]: transformerState.splitExpresList
        };
        break;
      case 'json':
        if (parseruleForm.depth || parseruleForm.depth == 0) {
          depthObj = {
            depth: parseruleForm.depth
          };
        }
        if (parseruleForm.keep) {
          keepObj = {
            keep: parseruleForm.keep
          };
        }
        expressionObj = {
          [parseruleForm.type]: parseruleForm.expression
            ? parseruleForm.expression
                .split(';')
                .toString()
                .split(',')
                .map(item => item.trim())
            : ''
        };
        break;
      case 'hebeipower':
        expressionObj = {
          plugin_type: parseruleForm.type,
          plugin_params: parseruleForm.expression
        };
        break;
      default:
        // regex udt
        expressionObj = {
          [parseruleForm.type]: parseruleForm.expression
        };
        break;
    }

    topParser = {
      parser: {
        parse: sourceForm.type == 'csv' ? {} : {
          [sourceForm.type == 'mqtt' ? 'payload' : 'value']: {
            ...expressionObj,
            ...depthObj,
            ...keepObj
          }
        }
      },
      input: sourceForm.type == 'csv' ? transformerState.csvTransformerParser?.inputList : generateInput()
    };
  }
  return topParser;
}
// 调用 flat 接口处理除了 mapping 的结果
async function handleParseResult(topParser: TopParseType) {
  const checkResult = checkParseData(topParser);
  if (checkResult) {
    ElMessage.warning(t(checkResult));
    return;
  }

  transformerState.topParse = topParser;
  const result = await dataInProps.transform.api.getParser(topParser);
  if (result.message) {
    ElMessage.error(result.message);
    isbreak.value = true;
    return;
  }
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
  isbreak.value = false;
  let hiddenCols: string[] = [];
  if (sourceForm.type == 'mqtt') {
    hiddenCols = defaultColsMap.mqtt;
  } else if (sourceForm.type == 'kafka') {
    hiddenCols = defaultColsMap.kafka;
  } else if (sourceForm.type == 'mongodb') {
    hiddenCols = defaultColsMap.mongodb;
  }

  let resultTableData: any[] = [];
  resultTableData = supportTransform.is_sparkplugb
    ? result
        .map((result: any) => {
          return result.columns.map((data: any) => {
            return Object.fromEntries(
              result.fields
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
                .filter((f: string[]) => !hiddenCols.includes(f[0]))
            );
          });
        })
        .flat(Infinity)
    : result[0].columns.map((data: any) => {
        return Object.fromEntries(
          result[0].fields
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
            .filter((f: string[]) => !hiddenCols.includes(f[0]))
        );
      });

  transformerState.transformResultTable = resultTableData;
  transformerState.activeColumns = [];
  transformerState.resultCurrentPage = 1;

  const tbdata = result[0].columns.map((data: any) => {
    return Object.fromEntries(
      result[0].fields
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
        .filter((f: string[]) => !hiddenCols.includes(f[0]))
    );
  });
  columnsArr.value = (
    sourceForm.type == 'csv'
      ? result[0].fields
      : result[0].fields.filter((item: { name: string }) => {
          if (sourceForm.type == 'mqtt' && !defaultColsMap.mqtt.includes(item.name)) {
            return item;
          } else if (sourceForm.type == 'kafka' && !defaultColsMap.kafka.includes(item.name)) {
            return item;
          } else if (sourceForm.type == 'mongodb' && !defaultColsMap.mongodb.includes(item.name)) {
            return item;
          } else if (supportTransform.supportSQL || supportTransform.is_sparkplugb) {
            return item;
          }
        })
  ).map((val: any) => {
    const finalVal = tbdata.map((item: any) => {
      return item[val.name];
    });

    return {
      description: val.name,
      name: val.name,
      show: true,
      type: !val.arrow_type?.List ? 'string' : 'array',
      localType: val.type,
      value: t('dataIn.transformer.sampleval') + ':' + (finalVal.join('') ? finalVal.join(' ; ') : '')
    };
  });
  if (!transformerState.transResultName) {
    transformerState.transResultName = '';
  }
  transformerState.stbDefaultColumns = columnsArr.value;
}
// 处理 mapping 的结果
async function getParserData(data: TransformerfullparamsType | TransformerSpbfullparamsType) {
  try {
    const checkResult = checkParseData(data);
    if (checkResult) {
      ElMessage.warning(t(checkResult));
      return;
    }
    const result = await dataInProps.transform.api.getParser(data);
    if (result.message) {
      ElMessage.error(result.message);
      isbreak.value = true;
      return;
    }
    isbreak.value = false;

    // 预览映射结果table数据
    let resultTableData: any[] = [];
    resultTableData = result.map((item: Recordable) => {
      const fields = item.fields;
      const columns = item.columns;
      const fieldNames = fields.map((field: Recordable) => field.name);

      return columns.map((row: any) => {
        // 为每一行数据创建一个字典，字段名作为键，行数据作为值
        const rowDict: Recordable = {};
        fieldNames.forEach((fieldName: string, index: number) => {
          rowDict[fieldName] = filterEmpty(row[index]);
        });

        if (sourceForm.type == 'mqtt' || sourceForm.type == 'sparkplugb') {
          rowDict.SuperTableName = rowDict['__using__'];
        }
        rowDict.SubTableName = rowDict['__tbname__'];
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { __using__, __tbname__, ...rest } = rowDict;

        // 统一列顺序: SuperTableName, SubTableName, 其余字段（按原解析顺序）
        const finalRow: Recordable = {};
        if (rowDict.SuperTableName !== undefined) {
          finalRow.SuperTableName = rowDict.SuperTableName;
        }
        if (rowDict.SubTableName !== undefined) {
          finalRow.SubTableName = rowDict.SubTableName;
        }
        // 其余字段保持原 fields 顺序（排除已处理和内部字段）
        fieldNames.forEach((k: string) => {
          if (!['__using__', '__tbname__', 'SuperTableName', 'SubTableName'].includes(k)) {
            if (k in rest) finalRow[k] = rest[k];
          }
        });
        // 若解析结果里后来追加的新字段（非常规）也需要保留
        Object.keys(rest).forEach(k => {
          if (!(k in finalRow)) {
            finalRow[k] = rest[k];
          }
        });

        return finalRow;
      });
    });

    transformerState.transformResultTable = resultTableData;
    nextTick(() => {
      transformerState.showResultTb = true;
      transformerState.resultTbTitle = 'mappingResTb';
      transformerState.transResultName = 'mappping';
    });

    setPageTableData();
  } catch (error) {
    console.log(error);
  }
}
function handelExtractArr(columnsArr: any[], extractArr: Recordable) {
  const names = columnsArr.map(obj => obj.name);
  // 过滤 extractArr，移除不在 names 中的对象
  const arr = extractArr.filter((obj: Recordable) => names.includes(obj.columnname));
  extractArr.value = arr;
}
function updateExtractArr(index: number, filterName: string) {
  extractArr.value[index]['type'] = filterName;
}
function setPageTableData() {
  pageTableData.value = tableData.value.slice(
    (currentPage.value - 1) * pageSize.value,
    currentPage.value * pageSize.value
  );
}
function onDefaultValueInput(name: any, val: string, range: number[]) {
  if (val === undefined || val.trim() === '') {
    setDefaultValueError(name, '');
    return;
  }

  if (range[2] <= 20) {
    // 整数
    if (val.indexOf('.') >= 0 || isNaN(Number(val)) || val.length > range[2]) {
      alertDataRange(name, val, range);
      return;
    }
    let ival;
    if (range[2] < 20) {
      ival = parseInt(val);
    } else {
      ival = eval(val + 'n');
    }
    if (ival < range[0] || ival > range[1]) {
      alertDataRange(name, val, range);
      return;
    }
  } else {
    // 浮点数
    if (isNaN(Number(val)) || val.length > range[2]) {
      alertDataRange(name, val, range);
      return;
    }
    const fval = parseFloat(val);
    if (fval < range[0] || fval > range[1]) {
      alertDataRange(name, val, range);
      return;
    }
  }

  setDefaultValueError(name, '');
}
function alertDataRange(name: any, _val: string, range: number[]) {
  const dataRangeInputTip = t('dataIn.transformer.dataRangeInputTip', [range[0], range[1]]);
  setDefaultValueError(name, dataRangeInputTip);
}

function setDefaultValueError(name: string, errorMsg: string) {
  pageTableData.value.forEach(item => {
    if (item.Name === name) {
      item.defaultValueError = errorMsg;
    }
  });
}

function handleCurrentChange(val: number) {
  currentPage.value = val;
  pageTableData.value.splice(0, Infinity);
  setPageTableData();
}

//编辑回显数据--编辑状态不自动显示result table
async function echoParser(parse: TransformerfullparamsType | TransformerSpbfullparamsType | null) {
  if (supportTransform.supportSQL || sourceForm.type == 'sparkplugb') {
    const params: Recordable = { dsn: sourceForm };
    params.sample_data_limit = transformerState.limitOffset;
    const result = await dataInProps.transform.api.getSampleDataMsgbody(params);
    if (result && Object.hasOwnProperty.call(result, 'code')) {
      ElMessage.error(result.message);
      msgForm.msgbody = '';
      return;
    }
    msgForm.msgbody = JSON.stringify(result);
  } else {
    let csvechoTransData = null;
    currentPage.value = parse?.format?.currentPage as number;
    if (sourceForm.type == 'csv') {
      isCSV.value = true;
      csvechoTransData = transformerState.csvTransformerParser;
      const columns = csvechoTransData?.columns?.map(item => {
        return {
          description: item,
          name: item,
          type: 'varchar',
          value: ''
        };
      }) as Recordable[];
      initColumnLists(columns);
    }

    const parseData = parse as TransformerfullparamsType | null;
    msgForm.msgbody =
      sourceForm.type == 'mqtt'
        ? parseData?.input.map(item => item.payload).join(' ') || ''
        : isCSV.value
          ? csvechoTransData?.msgBody || ''
          : parseData?.input.map(item => item.value).join(' ') || '';
    // 回填解析 topic 的值
    if (supportTransform.supportTopicBody) {
      parseData?.input.map(item => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { payload, ...rest } = item;
        msgForm.topicbody.push(rest);
      });
    }

    let tagKey = '';
    switch (sourceForm.type) {
      case 'mqtt':
        tagKey = 'payload';
        break;
      case 'sparkplugb':
        break;
      default:
        tagKey = 'value';
        break;
    }
    if (tagKey !== '') {
      const keys = Object.keys(parse?.parser.parse[tagKey]);
      if (keys.includes('plugin_type')) {
        parseruleForm.type = parse?.parser.parse[tagKey]['plugin_type'];
        parseruleForm.expression = parse?.parser.parse[tagKey]['plugin_params'];
      } else {
        parseruleForm.type = keys.filter(item => item != 'depth' && item != 'keep').toString();

        if (parseruleForm.type == 'json') {
          parseruleForm.depth = parse?.parser.parse[tagKey]['depth'];
          parseruleForm.keep = parse?.parser.parse[tagKey]['keep'];
        }
        parseruleForm.expression = parse?.parser.parse[tagKey][parseruleForm.type].toString();
      }
    }
  }

  await submitParse();

  echoExtractData(parse?.parser.mutate ?? []);
  const echoMapData = echoFilterData(parse?.parser.mutate ?? []);
  const transformEchoMapData = {
    model: parse?.parser.model,
    s_model: parse?.parser.s_model,
    tableData: echoMapData
  };
  echoMappingData(transformEchoMapData);
}
// 处理回显
function echoExtractData(mutate: Recordable[]) {
  const identifiedColObj = mutate.filter(item => {
    if (Object.keys(item).toString() == 'extract') {
      return item;
    }
  })[0] as TransformExtractParseDataType;

  if (identifiedColObj?.extract) {
    Object.entries(identifiedColObj.extract).forEach(item => {
      const ind = columnsArr.value.findIndex(col => col.name == item[0]);
      let convertData;
      if (supportTransform.supportTransform) {
        convertData = item[1];
      } else {
        convertData = JSON.stringify(Object.values(item[1])[0]);
      }

      const obj: Recordable = {
        columnname: item[0],
        expression: item[1].convert ? convertData : Object.values(item[1]).flat(1).join(';'),
        type: item[1].convert ? 'convert' : Object.keys(item[1]).toString(),
        columns: columnsArr.value,
        key: Math.random()
      };
      if (ind > -1) {
        columnsArr.value[ind]['show'] = false;
      }
      if (Object.keys(item[1]).toString() == 'split') {
        obj['splitParams'] = Object.keys(item[1]['split'])
          .map(k => {
            return {
              [k]: String(item[1]['split'][k])
            };
          })
          .reduce((a, b) => {
            a[Object.keys(b).toString()] = String(b[Object.keys(b).toString()]);
            return a;
          }, {});
      }
      if (item[1].convert) {
        obj['convertParams'] = {
          convert: JSON.stringify(item[1].convert),
          new_field_name: item[1].new_field_name
        };
        // obj['convertParams'] = Object.keys(['convert'])
        //   .map(k => {
        //     return {
        //       [k]: String(item[1]['convert'][k])
        //     };
        //   })
        //   .reduce((a, b) => {
        //     a[Object.keys(b).toString()] = String(b[Object.keys(b).toString()]);
        //     return a;
        //   }, {});
      }

      if (columnsArr.value.length > 0) {
        extractArr.value.push(obj);
      }
    });
    // transformerState.transformExtractParseData = mutate.extract？？？？？
    transformerState.transformExtractParseData = identifiedColObj;
  }
  nextTick(async () => {
    if (extractRef.value && extractRef.value.length > 0) {
      const newarr = [];
      for (let i = 0; i < extractRef.value.length; i++) {
        newarr.push(extractRef.value[i].submitExtract());
      }
      await extractRef.value[0].submitExtract(true);
      await Promise.all(newarr);
    }
  });
}
function echoFilterData(mutate: Recordable[]) {
  let echoMapData: Recordable[] = [];
  let isincludeFilter = false;
  mutate.forEach(item => {
    if (Object.keys(item).toString() == 'filter') {
      isincludeFilter = true;
      const obj = {
        expression: item.filter,
        key: Math.random()
      };
      filterArr.value.splice(0, filterArr.value.length, obj);
    }
    if (Object.keys(item).toString() == 'map') {
      echoMapData = (Object.entries(item['map']) as Recordable).map((val: any) => {
        const expreKey = Object.keys(val[1]).filter(key => key != 'as')[0];

        return {
          columnname: val[0],
          type: expreKey,
          expression: val[1][expreKey],
          default: val[1]['default'] || '',
          joinwith: val[1]['with'] || '',
          datatype: val[1]['as']
        };
      });
    }
  });
  nextTick(async () => {
    if (isincludeFilter) {
      await filterRef.value[0].submitFilter();
    }
  });

  return echoMapData;
}
function echoMappingData(transformEchoMapData: Recordable) {
  nextTick(async () => {
    sruleForm.s_name = transformEchoMapData.model?.using;
    transformerState.s_model = transformEchoMapData.s_model;
    await getSTbaleList(true, !!transformEchoMapData.s_model, transformEchoMapData);
    echoFetchMap(transformEchoMapData);
    if (sourceForm.type !== 'csv' && !supportTransform.supportSQL) {
      selectJson();
    }
    // transformerState.showResultTb = false;
    // transformerState.transResultName = '';
  });
}
//回显数据调用mapping接口
function echoFetchMap(echoData: Recordable) {
  if (echoData) {
    //编辑回显
    tableData.value.map((item: any) => {
      if (echoData.tableData.map((v: any) => v.columnname).includes(item['Name'])) {
        const idx = echoData.tableData.findIndex((val: any) => val.columnname == item['Name']);
        item.maptype = []
          .concat(item.maptype[0])
          .concat(
            echoData.tableData[idx].type == 'cast' ? echoData.tableData[idx].expression : echoData.tableData[idx].type
          );
        item.exprname = echoData.tableData[idx].type == 'cast' ? 'mapping' : echoData.tableData[idx].type;
        item['Expression'] = ['sum', 'join'].includes(item.exprname)
          ? echoData.tableData[idx].expression
          : echoData.tableData[idx].expression.toString();

        if (echoData.tableData[idx].default) {
          // this.$set(item, 'default', echoData.tableData[idx].default);
          item['default'] = echoData.tableData[idx].default;
        }
        if (echoData.tableData[idx].joinwith) {
          // this.$set(item, 'joinwith', echoData.tableData[idx].joinwith);
          item['joinwith'] = echoData.tableData[idx].joinwith;
        }
      }
      return item;
    });
    if (tableData.value[0]) {
      tableData.value[0]['Expression'] = echoData.model.name.toString();
    }
    clearTargetTBWhenDelete();
    caculateMappingResult();
  }
}
function clearTargetTBWhenDelete() {
  // 数据库为空
  if (!sourceForm.targetDB) {
    //  || !this.stableLists.find((v) => v === sruleForm.s_name)
    clearStbMapping();
  }
}
function clearStbMapping() {
  sruleForm.s_name = '';
  tableData.value = [];
  params_tags.value = [];
  setPageTableData();
}
//初始化列下拉框数据，适用于新增和编辑，拷贝
function initColumnLists(columns: Recordable[]) {
  indentifiedColumns.value = columns.map(item => {
    return {
      ...item,
      show: true
    };
  });
}
async function validateTransform() {
  const msgflag = await validateMsgBody();
  const stableflag = await validateTargetStb();
  if (msgflag && stableflag) {
    return true;
  }

  return false;
}

//messagebody非空验证触发
async function validateMsgBody(): Promise<boolean> {
  return new Promise(resolve => {
    msgFormRef.value?.validate((valid: boolean) => {
      resolve(valid);
    });
  });
}

async function validateTargetStb(): Promise<boolean> {
  return new Promise(resolve => {
    sruleFormRef.value?.validate((valid: boolean) => {
      resolve(valid);
    });
  });
}

//计算mapping的结果
async function caculateMappingResult() {
  if (!(await validateTransform())) {
    isbreak.value = true;
    return;
  }
  nextTick(() => {
    document.querySelector('.common-transformer .el-form-item__error')?.scrollIntoView();
    return;
  });
  if (!msgForm.msgbody) {
    isbreak.value = true;
    return false;
  }

  if (tableData.value && !tableData.value[0]?.['Expression'] && sruleForm.s_name) {
    ElMessage.warning(t('dataIn.transformer.tablenametip'));
    isbreak.value = true;
    return false;
  }
  isbreak.value = false;
  const tags: string[] = [];
  const columns: string[] = [];
  const commonColumns = [];
  const mutates: Recordable[] = [];
  const mutateMap = {};

  const precision_res = await executeSqlFn!(`
        select \`precision\` from information_schema.ins_databases where name = '${sourceForm.targetDB}'
        `);
  const precision = precision_res.data[0][0];

  tableData.value.forEach((item: Recordable) => {
    // 主键列不能为空
    if (item['PrimaryKey'] && !item['Expression']) {
      ElMessage.closeAll();
      ElMessage.warning(t('dataIn.transformer.mappingvaildtip'));
      isbreak.value = true;
    }
    // 不支持 GEOMETRY
    if (item['Type'] == 'GEOMETRY' && item['Expression']) {
      ElMessage.closeAll();
      ElMessage.warning(t('dataIn.transformer.nonsupportTypetip', ['GEOMETRY']));
      isbreak.value = true;
    }
    if (item['Expression']) {
      if (params_columns.value.includes(item['Name'])) {
        columns.push(item['Name']);
      }
      if (params_tags.value.includes(item['Name'])) {
        tags.push(item['Name']);
      }
      if (!item['PrimaryKey'] && params_columns.value.includes(item['Name'])) {
        commonColumns.push(item['Name']);
      }
      const key = item.exprname == 'mapping' ? 'cast' : item.exprname; //此处处理了编辑回显
      if (item['Type'] !== 'Tablename') {
        //排除第一行的tablename
        const expreitem = {
          [`${key}`]: ['sum', 'join'].includes(key) ? item['Expression'] : item['Expression'].toString().trim(),
          as: item['Type']
        };
        if (key == 'join') {
          expreitem['with'] = item.joinwith;
        }
        if (item.defaultValueError) {
          isbreak.value = true;
          ElMessage.error(t('data.fields') + '[' + item.Name + '],' + item.defaultValueError);
        }
        if (item.exprname == 'mapping' && params_columns.value.includes(item['Name'])) {
          if (item.default) {
            if (item.dataType === 'TIMESTAMP') {
              expreitem['default'] = item.default + '';
            } else {
              expreitem['default'] = item.default;
            }
          }
        }
        if (expreitem['generator'] === 'now') {
          expreitem['precision'] = precision;
        }
        mutates.push({
          [`${item['Name']}`]: expreitem
        });
      }
    }
  });
  if (isbreak.value) return;

  mutates.forEach(item => {
    Object.assign(mutateMap, item);
  });

  const parserData = {
    parse: transformerState.topParse?.parser?.parse,
    model: {
      name: tableData.value[0]?.['Expression'],
      using: sruleForm.s_name,
      tags: tags,
      columns: columns
    },
    mutate: transformerState.transformerFilterParseData
      ? transformerState.transformExtractParseData
        ? [
            { ...transformerState.transformExtractParseData },
            {
              filter: Object.values(transformerState.transformerFilterParseData).toString()
            },
            {
              map: mutateMap
            }
          ]
        : [
            {
              filter: Object.values(transformerState.transformerFilterParseData).toString()
            },
            {
              map: mutateMap
            }
          ]
      : transformerState.transformExtractParseData
        ? [
            { ...transformerState.transformExtractParseData },
            {
              map: mutateMap
            }
          ]
        : [
            {
              map: mutateMap
            }
          ]
  };
  const format = {
    pageCount: pageCount.value,
    pageSize: pageSize.value,
    currentPage: currentPage.value
  };

  let parserfullData;
  if (supportTransform.is_sparkplugb) {
    const topparse = transformerState.topParse as SpbTopParseType;
    parserfullData = {
      parser: parserData,
      samples: topparse.samples,
      format: format
    } as TransformerSpbfullparamsType;
  } else {
    const topparse = transformerState.topParse as TopParseType;
    const input = isCSV.value
      ? transformerState.csvTransformerParser?.inputList
      : supportTransform.supportSQL
        ? topparse?.input
        : generateInput();
    parserfullData = {
      parser: parserData,
      input: input,
      format: format
    } as TransformerfullparamsType;
  }

  // mqtt 用模版的方式创建超级表增加的参数
  if (JSON.stringify(transformerState.s_model) !== '{}') {
    parserfullData.parser['s_model'] = transformerState.s_model;
  }

  // 至少必须配置一个tag和一个column
  if (tags.length == 0 || commonColumns.length == 0) {
    ElMessage.closeAll();
    ElMessage.warning(t('dataIn.transformer.mappingvaildColtip'));
    isbreak.value = true;
    return;
  }
  isbreak.value = false;

  mappingParser = Object.assign(mappingParser, parserfullData);
  await getParserData(parserfullData);
}
//设置extract的name
function setExtractName(index: number, name: string) {
  extractArr.value[index]['columnname'] = name;
}
//给filter赋值
function changeFilter(key: any, value: string) {
  const index = filterArr.value.findIndex(val => val.key == key);
  filterArr.value[index]['expression'] = value;
}
//extract的expression赋值
function changeExtractExpr(colname: string, value: string) {
  const index = extractArr.value.findIndex((item: any) => item.columnname == colname);
  extractArr.value[index]['expression'] = value;
}
//获取transformer的所有参数
async function getTransformerParams() {
  await caculateMappingResult();
  if (isbreak.value) return;
  const parserDataParser = {
    global: getWriteConfigData(sourceForm.data),
    parse: transformerState.topParse?.parser.parse,
    model: mappingParser.parser.model,
    mutate: mappingParser.parser.mutate
  };
  const parserDataFormat = {
    pageCount: pageCount.value,
    pageSize: pageSize.value,
    currentPage: currentPage.value
  };
  let parserData;
  if (supportTransform.is_sparkplugb) {
    const topparse = transformerState.topParse as SpbTopParseType;
    parserData = {
      parser: parserDataParser,
      samples: topparse?.samples,
      format: parserDataFormat
    } as TransformerSpbfullparamsType;
  } else {
    const topparse = transformerState.topParse as TopParseType;
    parserData = {
      parser: parserDataParser,
      input: isCSV.value
        ? transformerState.csvTransformerParser?.inputList
        : supportTransform.supportSQL
          ? topparse?.input
          : generateInput(),
      format: parserDataFormat
    } as TransformerfullparamsType;
  }

  if (JSON.stringify(transformerState.s_model) !== '{}') {
    parserData.parser['s_model'] = transformerState.s_model;
  }

  transformerState.transformerfullparams = parserData;
  // this.$emit("getTransformerParams", parserData);
}
function changeColumnStatus(index: number, name: string) {
  //选中的列不能再选中
  const ind = columnsArr.value.findIndex(item => item.name == name);
  columnsArr.value[ind]['show'] = false;
  extractAddStatus.value = columnsArr.value.every(item => !item.show);
  extractArr.value[index]['columnname'] = name;
  extractArr.value[index]['value_type'] = columnsArr.value[ind].type;
}

provide('generateInput', generateInput);
//输出input结果
function generateInput() {
  let demo_list;
  try {
    if (parseruleForm.type == 'regex') {
      demo_list = msgForm.msgbody.split(/\n+/);
    } else {
      demo_list = getExampleList(msgForm.msgbody);
      // 如果 表达式不存在检查 json key 不能包含点
    }
  } catch (err: any) {
    if (err.lineNumber > 0) {
      ElMessage.error(t('dataIn.transformer.jsonDemoError', [err.lineNumber, err.message]));
    } else {
      ElMessage.error(err);
    }
  }
  if (!parseruleForm.expression && demo_list) {
    validateJsonKeys(demo_list);
  }

  let inputList = demo_list?.map(msg => {
    let inputobj: any;
    indentifiedColumns.value.forEach((item: any) => {
      if (msg) {
        if (sourceForm.type == 'mqtt') {
          inputobj = inputobj ? inputobj : {};
          if (item.name == 'payload') {
            inputobj['payload'] = msg;
          } else {
            inputobj[item.name] = item.type == 'timestamp' ? '' : item.name;
          }
        } else if (sourceForm.type == 'kafka' || sourceForm.type == 'mongodb') {
          inputobj = inputobj ? inputobj : {};
          if (item.name == 'value') {
            inputobj['value'] = msg;
          } else {
            inputobj[item.name] = item.type == 'timestamp' ? '' : item.name;
          }
        } else if (sourceForm.type == 'sparkplugb') {
          inputobj = JSON.parse(msg)['samples'];
        }
      }
    });
    return inputobj;
  });

  // mqtt 有主题解析时需要加上字段
  inputList = inputList?.map((item, index) => {
    const newItem = msgForm.topicbody[index];
    return { ...item, ...newItem };
  });

  return inputList?.filter(v => JSON.stringify(v) !== '{}');
}
function closeDialog() {
  dialogForm.st_name = '';
  showCreateDialog.value = false;
}
//创建或者查询
async function createStableSucc(stbName: string) {
  await getInitStables();
  sruleForm.s_name = stbName;
  getSTbaleList(false);
  closeDialog();
}
async function createTemplateStableSucc(stbName: string) {
  sruleForm.s_name = stbName;
  getSTbaleList(false, true);
  closeDialog();
}
//获取初始化的stables
async function getInitStables() {
  if (!sourceForm.targetDB) return;
  const sql = `show  \`${sourceForm.targetDB}\`.stables `;

  executeSqlFn!(sql, false)
    .then(data => {
      stableLists.value = Array.from(data.data).flat(1);
    })
    .catch(data => {
      ElMessage.closeAll();
      if (data.desc) {
        ElMessage.error(data.desc);
      }
    });

  if (isEditable.value) {
    clearTargetTBWhenDelete();
  }
}
function createStable(command: string) {
  if (!sourceForm.targetDB) {
    transformerState.createStWithoutDB = 1;
    return;
  } else {
    transformerState.createStWithoutDB = 2;
  }

  if (extractRef.value && extractRef.value.length > 0) {
    extractRef.value[extractRef.value.length - 1].submitExtract(true);
  }

  activeType.value = command;
  showCreateDialog.value = true;
  componentKey.value++;
}

async function getSTbaleList(isEcho: boolean, isTemplateCreate?: boolean, transformEchoMapData?: Recordable) {
  const col_models = transformEchoMapData?.tableData;
  const tags = {} as Recordable;
  transformEchoMapData?.model.tags.forEach(key => {
    tags[key] = true;
  });
  try {
    currentPage.value = 1;
    let res = {} as Recordable;

    if (isTemplateCreate) {
      res.data = convert(transformerState.s_model);
    } else {
      try {
        res = await executeSqlFn!(`desc \`${sourceForm.targetDB}\`.\`${sruleForm.s_name}\``);
        if (res.desc) {
          ElMessage.error(res.desc);
          return;
        }
      } catch (error) {
        console.log(error);
        res.data = col_models
          ? col_models.map(val => {
              if (tags[val.columnname]) {
                return [val.columnname, val.datatype, 0, 'TAG', 'disabled', 'disabled', 'disabled'];
              } else {
                return [val.columnname, val.datatype, 0, '', '', '', ''];
              }
            })
          : {};
      }
    }
    const precision = await executeSqlFn!(`
        select \`precision\` from information_schema.ins_databases where name = '${sourceForm.targetDB}'
        `);

    if (!isEmpty(transformerState.transformerMapCloumns)) {
      options.value = transformerState.transformerMapCloumns;
      mappingcolumns.value = (transformerState.transformerMapCloumns as any[])
        .filter(item => item.value === 'mapping')[0]
        .children.filter((val: any) => val);
    }

    const defaultmap =
      options.value
        .filter((item: Recordable) => item.value == 'mapping')[0]
        ?.children.map((child: Recordable) => child.label) || [];
    params_columns.value.splice(0, params_columns.value.length - 1);
    params_tags.value.splice(0, params_tags.value.length - 1);
    pageCount.value = res.data.length + 1;

    tableData.value = res.data.map((val: string[], index: number) => {
      const tableRow = { Name: val[0], exprname: 'mapping' } as TableRow;
      if (val[3] !== 'TAG' && index > 0) {
        params_columns.value.push(val[0]); //存储非主键列
        const dataRange = getDataRange(val[1]);
        dataRange && (tableRow.dataRange = dataRange);
        tableRow.dataType = val[1];
      }
      if (val.includes('TAG')) {
        params_tags.value.push(val[0]);
      }
      const equalindex = defaultmap.findIndex((item: string) => item.toLowerCase() == val[0].toLowerCase());

      tableRow.Type = val[1] == 'TIMESTAMP' ? val[1] + '(' + precision.data[0][0] + ')' : val[1];
      tableRow.maptype = equalindex > -1 ? ['mapping', `${defaultmap[equalindex]}`] : ['expression', 'value'];
      tableRow.Expression = equalindex > -1 && !isEcho ? defaultmap[equalindex] : '';
      tableRow.PrimaryKey = val[3] == 'PRIMARY KEY' || (val[1].includes('TIMESTAMP') && !index);

      return tableRow;
    });

    tableData.value.unshift({
      Name: 'SubTableName', //sruleForm.s_name,
      Type: 'Tablename',
      exprname: 'mapping',
      maptype: ['expression', 'string'],
      Expression: ''
    });
    params_columns.value.unshift(res.data[0][0]);
    setPageTableData();
  } catch (error) {
    console.log(error);
  }
}

//新增extract
function addNewExtract() {
  extractArr.value.push({
    columns: columnsArr.value,
    columnname: '',
    expression: '',
    type: '',
    key: Math.random(),
    splitParams: {
      sep: '',
      n: ''
      // names: ''
    },
    convertParams: {
      convert: '',
      new_field_name: ''
    }
  });
}
//新增filter
function addNewFilter() {
  filterArr.value.push({
    expression: '',
    key: Math.random()
  });
}
//删除filter
function deleteFilter(key: number) {
  ElMessageBox.confirm(t('dataIn.deletetip'), t('dataIn.warning'), {
    confirmButtonText: t('dataIn.ok'),
    cancelButtonText: t('dataIn.cancel'),
    type: 'warning'
  }).then(() => {
    const ind = filterArr.value.findIndex((val: Recordable) => val.key == key);
    filterArr.value.splice(ind, 1);
    transformerState.transformerFilterParseData = null;

    if (extractRef.value && extractRef.value.length > 0) {
      extractRef.value[extractRef.value.length - 1].submitExtract(true);
    } else {
      submitParse();
    }
  });
}
function deleteExtract(index: number, name: string) {
  if (!name) {
    // 没有设置name的情况下，直接删除
    extractArr.value.splice(index, 1);
    return;
  }

  ElMessageBox.confirm(t('dataIn.deletetip'), t('dataIn.warning'), {
    confirmButtonText: t('dataIn.ok'),
    cancelButtonText: t('dataIn.cancel'),
    type: 'warning'
  }).then(() => {
    if (transformerState.transResultName == name) {
      // 删除提取拆分列时 transResultName 更改为当前删除name,预览表格才能展示正确
      transformerState.transResultName = name;
    }
    const oldextract = transformerState.transformExtractParseData;

    if (oldextract && Object.keys(oldextract.extract).includes(name)) {
      delete oldextract.extract[name];
    }

    const ind = extractArr.value.findIndex((item: any) => item.columnname == name);
    extractArr.value.splice(ind, 1);
    const restoreIndex = columnsArr.value.findIndex(item => item.name == name);
    columnsArr.value[restoreIndex]['show'] = true;

    if (extractArr.value.length > 0) {
      if (filterArr.value.length > 0 && filterRef.value[0].isexecuted) {
        filterRef.value[0].submit();
      } else {
        extractRef.value[0].submitExtract();
        extractRef.value[0].submitExtract(true);
      }
    } else {
      transformerState.transformExtractParseData = null;
      filterArr.value.splice(0, 1);
      transformerState.transformerFilterParseData = null;
      if (filterArr.value.length > 0 && filterRef.value[0].isexecuted) {
        filterRef.value[0].submit();
      } else {
        submitParse();
      }
    }
  });
}
//-----------------------处理csv部分
//组合csv的extract
function formatCSVExtract(columns: Recordable[]) {
  columnsArr.value = columns.map(item => {
    return {
      description: item,
      name: item,
      show: true,
      type: 'varchar',
      value: ''
    };
  });
  indentifiedColumns.value = columns.map(item => {
    return {
      description: item,
      name: item,
      show: true,
      type: 'varchar',
      value: ''
    };
  });
}

function updateData(data: string) {
  parseruleForm.expression = data;
}
function selectJson() {
  let propertiesStrArr: string[] = [];
  try {
    if (parseruleForm.type == 'json') {
      propertiesStrArr = extractAllProperties(msgForm.msgbody, parseruleForm.depth);
    }
  } catch (err: any) {
    if (err.lineNumber > 0) {
      ElMessage.error(t('dataIn.transformer.jsonDemoError').replace('{0}', err.lineNumber).replace('{1}', err.message));
    } else {
      ElMessage.error(t(err));
    }
  }

  if (parseruleForm.expression && parseruleForm.type == 'json') {
    // 回显逻辑
    const firstSplitArr = parseruleForm.expression.split(',');
    const checkedKey: string[] = [];
    const checkedObj: Recordable = {};
    firstSplitArr.map(item => {
      const splitArr = item.split('=');
      checkedKey.push(splitArr[0]);
      checkedObj[splitArr[0]] = splitArr[1];
    });

    allProperties.value = propertiesStrArr.map(item => {
      return {
        defaultValue: item,
        rename: checkedObj[item] ? checkedObj[item] : checkedKey.includes(item) ? '' : handleRename(item),
        checked: checkedKey.includes(item)
      };
    });
  } else {
    allProperties.value = propertiesStrArr.map(item => {
      return {
        defaultValue: item,
        rename: handleRename(item),
        checked: false
      };
    });
  }
}
function handleRename(value: string) {
  return value.replaceAll('"]["', '_').replace('$["', '').replace('"]', '');
}
function handleTypeChange() {
  parseruleForm.expression = '';
  parseruleForm.depth = undefined;
  parseruleForm.keep = false;
}

defineExpose({
  getTransformerParams,
  isbreak
});
</script>
<style lang="scss" scoped>
$color-description: rgb(137 130 130);

@keyframes heart {
  0% {
    box-shadow: 0 0 5px #4259ce;
  }

  50% {
    box-shadow: 0 0 20px #4259ce;
  }

  100% {
    box-shadow: 0 0 5px #4259ce;
  }
}

:deep(i) {
  font-size: 16px;
}

:deep(.btn-icon-small i) {
  font-size: 12px;
}

.mapping {
  margin-bottom: 15px;
  font-size: 16px;
  font-weight: 600;
  color: #4259ce;
}

.josn-wrap {
  display: inline-flex;
  align-items: start;
  width: 100%;

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

.block-title {
  margin-top: 15px;
  margin-bottom: 10px !important;
  font-size: 16px;
  font-weight: 600;
  color: #4259ce;

  &.sub {
    display: flex;
    justify-content: space-between;

    span {
      font-size: 14px !important;
    }

    .prew {
      cursor: pointer;
    }
  }
}

.col-list {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  gap: 15px;
  max-height: 200px;
  margin-top: 15px;
  margin-bottom: 20px;
  overflow-y: auto;

  li {
    color: #4259ce;
    text-align: center;
    background: #ecf2fe;
    border: 1px solid #f6f8fa;
    border-radius: 14px;
  }

  .col.origin {
    color: #fff;
    background: #409eff;
  }
}

.extract,
.filter {
  .el-button {
    width: 100%;
  }

  :deep(.el-input) {
    margin-left: 0 !important;
  }
}

.table-title {
  display: flex;
  align-items: center;
  white-space: nowrap;

  .title {
    display: flex;
    flex: 1;
    align-items: center;

    .el-form-item {
      margin-bottom: 0;
    }

    .el-select {
      width: 100%;
    }

    .el-form {
      flex: 1;
      margin-right: 15px;
      margin-left: 15px;
    }
  }
}

.table-detail {
  margin-top: 10px;

  .mapping {
    display: flex;
    justify-content: flex-end;
  }

  .box-expression {
    display: flex;
    flex-wrap: wrap;

    .mapping-rule-select {
      width: 100px;
      margin-right: 5px;
    }

    .mapping-rule-expression {
      flex: 1;
    }

    .mapping-rule-extra {
      width: 100px;
      margin-left: 5px;
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

  :deep(.el-table) {
    thead tr th {
      background-color: #f5f7fa;
    }

    .el-table__cell {
      padding: 6px 0 !important;
    }

    .el-form-item__error {
      top: 30%;
      left: 130px;
    }
  }

  :deep(.cell.el-tooltip) {
    // height: 40px;
    padding-right: 20px;
  }

  :deep(.el-form-item) {
    margin-right: 10px;
    margin-bottom: 0;
  }
}

.upload-demo {
  display: flex;
  flex: 1;
  align-items: baseline;

  :deep(.el-upload) {
    width: 100%;

    .el-button {
      width: 100%;
    }
  }
}

:deep(.el-input-group__prepend) {
  padding: 0 4px;
}

.block-page {
  position: relative;
  z-index: 101;
  display: flex;
  justify-content: space-between;
  margin-top: 15px;
}

.pagination {
  display: flex;
  margin-top: 0 !important;

  &.hide {
    :deep(.el-pagination__jump),
    :deep(button),
    :deep(.el-pager) {
      display: none;
    }
  }
}

.extrac-parse {
  display: flex;

  :deep(.el-form) {
    display: flex !important;
    flex: 1;
    align-items: flex-start;

    .el-form-item {
      margin-right: 15px;
      margin-bottom: 0;

      &:nth-child(2) {
        flex: 1;
      }
    }
  }

  :deep(.el-button) {
    display: flex;
    align-items: center;
    justify-content: center;
    width: auto;
    width: 32px;
    height: 32px;
    padding: 12px 20px;
    border-radius: 6px;
  }
}

.extract-btns {
  display: flex;

  :deep(.el-button) {
    margin-top: 10px;
  }
}

.extract-table {
  margin-top: 20px;
}

.mt5 {
  margin-top: 9px;
}

.msg-right {
  display: flex;
  flex-wrap: wrap;

  :deep(.el-button) {
    flex: 1;
  }
}

::v-deep {
  .el-input-number__increase,
  .el-input-number__decrease {
    height: 14px !important;
  }
}

.msg-sec {
  margin-bottom: 25px;

  :deep(.el-input-number--default) {
    width: 86px;
  }
}

.my-checkbox {
  display: block;
  margin-bottom: 5px;
}

.transform-json-icon {
  flex-shrink: 0;
  width: 16px;
  height: 16px;
  margin-top: 4px;
}

.udt {
  margin-bottom: 16px;
}
</style>
