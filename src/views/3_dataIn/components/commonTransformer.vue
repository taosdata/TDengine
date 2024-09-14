<template>
  <div class="common-transformer">
    <template>
      <section class="msg_sec">
        <div
          class="block-title"
          v-if="$store.state.app.supportSQL"
        >
          <span>{{ $t("datasource.transformer.msgbody") }}</span>
        </div>
        <el-row class="mt10">
          <el-col
            :span="$store.state.app.currentDBType == 'csv' ? 24: 17"
            >
              <el-form
                @submit.native.prevent
                :model="msgForm"
                ref="msgForm"
                >
                <el-form-item
                  prop="msgbody"
                  >
                  <el-input
                    :disabled="!!$store.state.app.supportSQL"
                    class="msgbody"
                    v-model="msgForm.msgbody"
                    :placeholder="$t('datasource.transformer.msgbodytip')"
                    size="small"
                    type="textarea"
                    :autosize="{ minRows: 7, maxRows: 7 }"
                  ></el-input>
                </el-form-item>
              </el-form>
          </el-col>
          <el-col :span="7" v-if="$store.state.app.currentDBType !== 'csv'" style="padding-left: 8px">
            <el-col class="flexBetween">
              {{
                $t("datasource.transformer.dataLimit")
              }}
              <el-input-number v-model="limitOffset" size="small" :min="1" :max="100" controls-position="right" @change="handleLimit"></el-input-number>
            </el-col>
            <el-col :class="['flexBetween','mt5']" v-if="$store.state.app.supportSQL">
              {{
                $t("datasource.transformer.timeout")
              }}
              <el-input-number v-model="timeout" size="small" :min="1" :max="600" controls-position="right" @change="handleLimit"></el-input-number>
            </el-col>
            <el-col
              name="second"
              :class="['mt5','msg-right']"
            >
              <el-tooltip
                placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
              >
                <template slot="content">
                  <span v-html="$t('communityTip')"></span>
                </template>
                <el-button type="primary" plain size="small" :loading="requesting" :disabled="$COMMUNITY" @click="getMsgBody">{{
                  $t("datasource.transformer.msgbodytypes.retrieve")
                }}</el-button>
              </el-tooltip>
            </el-col>
            <el-col
              name="third"
              :class="['mt5','msg-right']"
              v-if="!$store.state.app.supportSQL"
            >
              <el-tooltip
                  placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
                >
                  <template slot="content">
                    <span v-html="$t('communityTip')"></span>
                  </template>
                  <el-upload
                    class="upload-demo"
                    :action="uploadUrl"
                    :data="{req_id: 'taosx-demo-file'}"
                    :before-remove="beforeRemove"
                    :on-success="handleSuccess"
                    :on-progress="handleStart"
                    :on-error="handleError"
                    :on-exceed="handleExceed"
                    :file-list="fileList"
                    :show-file-list="false"
                  >
                    <el-button size="small" type="primary" plain :loading="request" :disabled="$COMMUNITY">{{
                      $t("datasource.transformer.msgbodytypes.type3")
                    }}</el-button>
                  </el-upload>
              </el-tooltip> 
            </el-col>
            <el-col
              name="first"
              :class="['mt5','msg-right']"
              v-if="!$store.state.app.supportSQL"
            >
            <el-button size="small" @click="clearMsgBody">{{
                $t("datasource.transformer.msgbodytypes.type1")
              }}</el-button>
            </el-col>
          </el-col>

        </el-row>
      </section>
      <section class="extract">
        <div class="block-title top">
          <span>{{
            $store.state.app.currentDBType == "csv" || $store.state.app.supportSQL
              ? $t("datasource.transformer.identified")
              : $t("datasource.transformer.parse")
          }}</span>
          <el-popover placement="top" effect="light" trigger="hover" width="520" v-model="visiblePop1">
            <div style="position: relative">
              <i style="position: absolute; right: 0px" class="el-icon-close" @click="handleClickPop('1')"></i>
              <DocsContent
                :style="docsStyle"
                :content="$t('datasource.transformer.extractdesc')"
              />
            </div>
            <span style="margin-left: 1px"
              slot="reference"
              v-if="!$store.state.app.supportSQL && $store.state.app.currentDBType !== 'csv'"
              ><Icon name="label_info" class="info_icon_custom"></Icon>
            </span>
          </el-popover>
        </div>
        <div
          class="extrac-parse"
          v-if="$store.state.app.currentDBType !== 'csv' && !$store.state.app.supportSQL"
        >
          <el-form :rules="parseRules" :model="parseruleForm">
            <el-form-item prop="type">
              <el-select
                size="small"
                :placeholder="$t('datasource.transformer.filter_type')"
                v-model="parseruleForm.type"
                @change="handleTypeChange"
              >
                <el-option
                  v-for="item in parseTypes"
                  :key="item"
                  :label="item"
                  :value="item"
                ></el-option>
              </el-select>
            </el-form-item>
            <el-form-item prop="expression">
              <template v-if="parseruleForm.type == 'split'">
                <SplitExpression ref="splitExpression"></SplitExpression>
              </template>
              <el-input
                v-else-if="parseruleForm.type == 'regex'"
                v-model="parseruleForm.expression"
                :placeholder="
                  parseruleForm.type == 'json'
                    ? 'key1,key2,key3=key3_alias'
                    : '(?<y>[0-9]{4})-(?<m>[0-9]{2})-(?<d>[0-9]{2})'
                "
                size="small"
                :disabled="parseruleForm.type == 'json'"
              >
              </el-input>
              <div
                class="josn-wrap"
                v-else-if="parseruleForm.type == 'json'"
              >
                <span>depth </span>
                <el-input-number
                  v-model="parseruleForm.depth"
                  style="width: 100px;margin-right: 5px"
                  size="small"
                  :controls="false"
                  :min="0"
                >
                </el-input-number>
                <cusSelect
                  v-model="parseruleForm.expression"
                  :allProperties="allProperties"
                  :selectJson="selectJson"
                  @updateData="updateData"
                  :depth="parseruleForm.depth"
                />
              </div>
              <div v-else-if="parseruleForm.type == 'udt'" 
                  style="display: inline-flex; align-items: start; width: 100%;">
                <el-input 
                  size="small"
                  v-model="parseruleForm.expression"
                  type="textarea" 
                  :autosize="{ minRows: 1, maxRows: 7}"></el-input>
                <el-upload 
                  size="small" 
                  @click="createST" 
                  style="margin-left: 10px"
                  :action="uploadUrl"
                  :data="uploadData"
                  :before-remove="beforeRemove"
                  :on-success="handleSuccessUdt"
                  :on-error="handleError"
                  :file-list="fileList"
                  :show-file-list="false">
                  <el-button size="small" plain type="primary" style="width: auto; padding: 0 6px; margin-top: 0;" :disabled="$COMMUNITY">
                    {{ $t("datasource.transformer.uploadCode") }}
                  </el-button>
                </el-upload>
              </div>
              <el-input v-else
                v-model="parseruleForm.expression"
                size="small"
              >
              </el-input>
            </el-form-item>
            <el-tooltip
              placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
            >
              <template slot="content">
                <span v-html="$t('communityTip')"></span>
              </template>
              <el-button
                size="small"
                icon="el-icon-PREVIEW"
                @click="submitParse"
                style="display: flex"
                :disabled="
                  msgForm.msgbody == '' || 
                  ((parseruleForm.type == 'udt' || parseruleForm.type == 'regex') && parseruleForm.expression == '') || 
                  $COMMUNITY
                  "
              ></el-button>
            </el-tooltip>
          </el-form>
        </div>
      </section>
      <section v-if="columnsArr.length > 0">
        <ul
          :class="[
            'col-list',
            $store.state.app.transresultname ==
            $t('datasource.transformer.identified')
              ? 'active'
              : '',
          ]"
        >
          <template v-for="(item, index) in columnsArr">
            <li v-if="index < 9" :key="index">
              <span>{{ item.name }}</span>
            </li>
          </template>
          <li v-if="columnsArr.length > 9">
            <el-tooltip
              :content="$t('datasource.transformer.viewmore')"
              placement="top"
              effect="light"
              ><span @click="submitParse"
                ><i class="el-icon-more"></i
              ></span>
            </el-tooltip>
          </li>
        </ul>
      </section>
      <section class="extract">
        <div class="block-title top">
          <span>{{ $t("datasource.transformer.extract") }}</span>
          <el-popover placement="top" trigger="hover" width="520" v-model="visiblePop2">
            <div style="position: relative">
              <i style="position: absolute; right: 0px" class="el-icon-close" @click="handleClickPop('2')"></i>
              <DocsContent
                :style="docsStyle"
                :content="$t('datasource.transformer.subextractdesc')"
              />
            </div>
            <span style="margin-left: 1px"
              slot="reference"
              ><Icon name="label_info" class="info_icon_custom"></Icon>
            </span>
          </el-popover>
        </div>
        <template v-for="(item, index) in extractArr">
          <ExtractSplit
            ref="extract"
            :key="item.key"
            :itemData="item"
            :index="index"
            :extractColumns="item.columns"
            :indentifiedColumns="indentifiedColumns"
            @deleteExtract="deleteExtract"
            @selectColumn="changeColumnStatus"
            @setExtractName="setExtractName"
            @changeExtractExpr="changeExtractExpr"
          ></ExtractSplit>
        </template>
        <el-tooltip
          placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
        >
          <template slot="content">
            <span v-html="$t('communityTip')"></span>
          </template>
          <el-button 
            type="primary"
            icon="el-icon-plus" 
            size="small" 
            class="btn-icon-small"
            plain
            @click="addNewExtract" 
            :disabled="columnsArr.length == 0 || $COMMUNITY"
          >
            {{ $t("datasource.transformer.addExtract") }}
          </el-button>
        </el-tooltip>
      </section>
      <section class="filter">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.filter") }}</span>
          <el-popover placement="top" effect="light" trigger="hover" width="520" v-model="visiblePop3">
            <div style="position: relative">
              <i style="position: absolute; right: 0px" class="el-icon-close" @click="handleClickPop('3')"></i>
              <DocsContent
                :style="docsStyle"
                :content="$t('datasource.transformer.filterdesc')"
              />
            </div>
            <span style="margin-left: 1px"
              slot="reference"
              ><Icon name="label_info" class="info_icon_custom"></Icon>
            </span>
          </el-popover>
        </div>
        <template v-for="(item, index) in filterArr">
          <FilterExpression
            :key="index"
            :index="index"
            :itemData="item"
            :payload="msgForm.msgbody"
            :inputparamsColumns="columnsArr"
            :indentifiedColumns="indentifiedColumns"
            @deleteFilter="deleteFilter"
            @changeFilter="changeFilter"
            ref="filter"
          ></FilterExpression>
        </template>
        <el-tooltip
          placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
        >
          <template slot="content">
            <span v-html="$t('communityTip')"></span>
          </template>
          <el-button
              type="primary"
              icon="el-icon-plus"
              size="small"
              class="btn-icon-small"
              plain
              @click="addNewFilter"
              :disabled="filterArr.length >= 1 || columnsArr.length == 0 || $COMMUNITY"
            >
              {{ $t("datasource.transformer.addfilter") }}
          </el-button>
        </el-tooltip>
      </section>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.superconfig") }}</span>
        </div>
        <div class="table-content">
          <div class="table-title"  style="margin-bottom: 16px">
            <div class="title">
              <span style="color: #4259ce">
                {{ $t("datasource.transformer.targetSt") }}
              </span>
              <el-form :model="sruleForm" ref="sruleForm" :rules="srules">
                <el-form-item prop="s_name">
                  <el-select
                    v-model="sruleForm.s_name"
                    allow-create
                    default-first-option
                    size="small"
                    @change="() => getSTbaleList(false)"
                    :placeholder = "$store.state.app.currentDBName ? $t('datasource.transformer.stableSelectOrCreateTip') : $t('datasource.transformer.databaseSelectTip')"
                    :disabled="!$store.state.app.currentDBName || columnsArr.length === 0"
                  >
                    <el-option
                      v-for="(item, index) in stableLists"
                      :key="index"
                      :label="item"
                      :value="item"
                    ></el-option>
                  </el-select>
                </el-form-item>
              </el-form>
            </div>
            <el-tooltip
              placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
            >
              <template slot="content">
                <span v-html="$t('communityTip')"></span>
              </template>
              <el-button
                type="primary"
                class="btn-icon-small"
                size="small"
                icon="el-icon-plus"
                plain
                @click="createStable"
                :disabled="$store.state.app.currentDBName == '' || columnsArr.length === 0 || $COMMUNITY"
              >
                {{ $t("datasource.transformer.createstb") }}
              </el-button>
            </el-tooltip>
          </div>
          <div class="table-detail" v-if="tableData.length > 0">
            <el-table :data="pageTableData" border style="width: 100%">
              <el-table-column
                prop="Name"
                show-overflow-tooltip
                label="Name"
                width="180px"
              >
                <template slot-scope="scope">
                  <div style="display: flex; align-items: end">
                    <i
                      class="el-icon-success"
                      style="color: rgb(56, 155, 255); margin-right: 2px"
                      v-if="scope.row.Expression.toString()"
                    ></i>
                    <Icon
                      :name="'tag'"
                      class="console-tree-icon"
                      style="width: 20px; height: 20px"
                      v-if="params_tags.includes(scope.row['Name'])"
                    ></Icon>
                    <Icon
                      :name="'key'"
                      class="console-tree-icon"
                      style="width: 20px; height: 20px"
                      v-if="scope.row.PrimaryKey"
                    ></Icon>

                    <span>{{ scope.row["Name"] }}</span>
                  </div>
                </template>
              </el-table-column>
              <el-table-column
                prop="Type"
                show-overflow-tooltip
                label="Type"
                width="150px"
              ></el-table-column>
              <el-table-column
                prop="Expression"
                label="Expression"
              >
              <template slot="header">
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template slot="content">
                    <DocsContent
                      :style="docsStyle"
                      :content="$t('datasource.transformer.expressiondesc')"
                    />
                  </template>
                  <span>Expression <Icon name="label_info" class="info_icon_custom"></Icon></span>
                </el-tooltip>
              </template>
                <div class="box-expression" slot-scope="scope">
                  <template v-if="scope.row['Name'] == 'SubTableName'">
                    <el-input
                      size="small"
                      v-model="scope.row.Expression"
                      :placeholder="exprformat"
                    ></el-input>
                  </template>
                  <template v-else>
                    <el-select
                      size="small"
                      v-model="scope.row.exprname"
                      class="mapping-rule-select"
                      @change="changeCurrentMapExpr(scope)"
                    >
                      <el-option
                        v-for="item in mappingTypes"
                        :key="item"
                        :label="item"
                        :value="item"
                        >{{ item }}</el-option
                      >
                    </el-select>
            
                    <el-select
                      v-if="scope.row.exprname == 'mapping' || scope.row.exprname == 'sum' || scope.row.exprname == 'join'"
                      v-model="scope.row.Expression"
                      :placeholder="$t('datasource.transformer.coltip')"
                      :clearable="scope.row.exprname == 'mapping'"
                      size="small"
                      filterable
                      :key="Math.random()"
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
                      slot="reference"
                      :key="'expr'"
                      class="mapping-rule-expression"
                      @change="statisticCol"
                      v-model="scope.row.Expression"
                      :placeholder="
                        scope.row.exprname == 'format'
                          ? exprformat
                          : scope.row.exprname == 'expr'
                          ? exprexpression
                          : scope.row.exprname == 'value'
                          ? $t('datasource.transformer.valuetip')
                          : ''
                      "
                      size="small"
                      :disabled="scope.row['exprname'] == 'generator'"
                    ></el-input>

                    <el-input
                      v-if="scope.row.exprname == 'join'"
                      size="small"
                      :key="'exprjoin'"
                      class="mapping-rule-extra"
                      v-model="scope.row.joinwith"
                      style="height: 32px;"
                    >
                      <template slot="prepend">with</template>
                    </el-input>
                    <el-input
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataRange"
                      size="small"
                      type="number"
                      :placeholder="$t('datasource.transformer.defaultValuePlaceholder')"
                      :maxlength="scope.row.dataRange[2]"
                      :key="'default-value-of-' + scope.row['Name']"
                      v-model="scope.row.default"
                      class="mapping-rule-extra"
                      @blur="onDefaultValueInput(scope.row.Name, scope.row.default, scope.row.dataRange)"
                    >
                    </el-input>
                    <!-- <el-date-picker
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataType == 'TIMESTAMP'"
                      v-model="scope.row.default"
                      :placeholder="$t('datasource.transformer.defaultValuePlaceholder')"
                      :key="'default-value-of-' + scope.row['Name']"
                      value-format="timestamp"
                      type="datetime"
                      size="small"
                      class="mapping-rule-extra"
                    >
                    </el-date-picker> -->
                    <el-select 
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataType == 'BOOL'"
                      v-model="scope.row.default"
                      :placeholder="$t('datasource.transformer.defaultValuePlaceholder')"
                      :key="'default-value-of-' + scope.row['Name']"
                      size="small"
                      class="mapping-rule-extra"
                    >
                      <el-option label="true" value="true"></el-option>
                      <el-option label="false" value="false"></el-option>
                      <el-option label="null" value="null"></el-option>
                    </el-select>
                    <el-input
                      v-else-if="scope.row.exprname == 'mapping' && scope.row.dataType"
                      size="small"
                      :placeholder="$t('datasource.transformer.defaultValuePlaceholder')"
                      :key="'default-value-of-' + scope.row['Name']"
                      v-model="scope.row.default"
                      class="mapping-rule-extra"
                    ></el-input>
                  </template>
                  <div class="default-value-error" v-if="scope.row.defaultValueError">
                    {{ scope.row.defaultValueError }}
                  </div>
                </div>
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
                  <span
                    style="color: #16191f; font-weight: 400; margin-left: 6px"
                  >
                    {{ $t("datasource.transformer.configuredcount") }}
                    {{ configuredCount }}
                    {{ $t("datasource.transformer.unit") }}</span
                  >
                </div>
              </el-pagination>

              <el-button
                size="small"
                icon="el-icon-PREVIEW"
                @click="caculateMappingResult"
              ></el-button>
              
            </div>
          </div>
        </div>
      </section>
      <el-dialog
        :title="$t('datasource.transformer.create_st')"
        :visible.sync="showCreateDialog"
        width="1000px"
        center
        :append-to-body="true"
        @close="closeDialog"
        :close-on-click-modal="false"
      >
        <CreateSTB ref="createstb" :columnsArr="columnsArr"></CreateSTB>
        <div class="buttons">
          <el-button type="primary" size="small" @click="createST">
            {{ $t("create") }}
          </el-button>
          <el-button size="small" @click="closeDialog">
            {{ $t("cancel") }}
          </el-button>
        </div>
      </el-dialog>
      <!-- <el-dialog
        :title="$t('datasource.transformer.udtTip')"
        :visible.sync="dialogVisible"
        width="1000px"
        destroy-on-close
        :append-to-body="true"
        @close="closeDialog"
        :close-on-click-modal="false"
      >
        <el-input 
          type="textarea" 
          class="udt" 
          :autosize="{ minRows: 10, maxRows: 10}"
          v-model="parseruleForm.expression"></el-input>
        <div class="flexBetween">
          <a>下载示例代码</a>
          <span class="flexStart">
            <el-button size="small" @click="createST" plain type="primary">
              {{ $t("preview") }}
            </el-button>
            <el-upload 
              size="small" 
              @click="createST" 
              style="margin-left: 10px"
              :action="uploadUrl"
              :before-remove="beforeRemove"
              :on-success="handleSuccess"
              :on-error="handleError"
              :file-list="fileList"
              :show-file-list="false">
              <el-button size="small" plain type="primary">
                {{ $t("datasource.transformer.uploadCode") }}
              </el-button>
            </el-upload>
            <el-button size="small" type="primary" @click="closeDialog" style="margin-left: 10px">
              {{ $t("confirm") }}
            </el-button>
          </span>
        </div>
      </el-dialog> -->
    </template>
  </div>
</template>
<script>
import ExtractSplit from "./extractSplit.vue";
import FilterExpression from "./filterExpression.vue";
import { getParser, checkParseData, getSampleDataMsgbody, listParserPlugins } from "@/api/explorer/datain";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import CreateSTB from "./createSTB.vue";
import { createStableReq } from "@/api/gateway/data/stables";
import SplitExpression from "./splitExpression.vue";
import { getDsnData, getDataRange } from "../utils.js";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";
import { extractAllProperties, getExampleList } from "@/utils"
import cusSelect from "./cusSelect.vue";
import VersionMixin from "@/mixins/version";

const PARSER_BUILDIN = ["json", "regex", "udt"];

export default {
  name: "CommonTransformer",
  inject: ['sourceParent'],
  components: {
    ExtractSplit,
    FilterExpression,
    CreateSTB,
    SplitExpression,
    DocsContent,
    cusSelect
  },
  props: {
    parent: {
      type: Object,
      default: () => {
        return null;
      },
    },
    parserColumns: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  mixins: [VersionMixin],
  data() {
    return {
      mqttDefaultCols: ["topic", "qos", "payload"],
      kafkaDefaultCols: ["topic", "partition", "offset", "key", "value"],
      mongodbDefaultCols: ["value"],
      parseTypes: [],
      exprformat: "${c1}-${c2}:${c3}",
      exprexpression: "centigrade * 1.8 + 32",
      parseruleForm: {
        type: "json",
        expression: "",
        depth: undefined
      },
      configuredCount: 0,
      parseRules: {
        type: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.transformer.filter_type"),
          },
        ],
      },
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      pageSize: 20,
      pageCount: 10,
      currentPage: 1,
      isbreak: false, //tranformer创建是否出错
      isCSV: false,
      mapExpressionList: [
        "value",
        "generator",
        "join",
        "format",
        "sum",
        "expr",
      ],

      timestampExpr: "",
      options: [],
      mappingcolumns: [],
      msgForm: {
        msgbody: "",
      },
      params_columns: [],
      params_tags: [],
      mapType: "value",
      extractAddStatus: false,
      mappingTypes: [
        "mapping",
        "value",
        "generator",
        "join",
        "format",
        "sum",
        "expr",
      ],

      dialogForm: {
        st_name: "",
      },
      dialogRules: {
        st_name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.st_input"),
          },
        ],
      },

      showCreateDialog: false,
      stableLists: [],
      sruleForm: {
        s_name: "",
      },
      uploadData: {
        req_id: new Date().getTime(),
      },
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      payloads: ["json", "csv"],
      fileList: [],
      ruleForm: {
        payload: "csv",
        file: "",
      },
      rules: [
        {
          payload: [
            {
              required: true,
              trigger: "blur",
            },
          ],
        },
      ],
      parseIndetntifiedCols: [],
      indentifiedColumns: [],
      columnsArr: [],
      tableData: [],
      pageTableData: [],
      extractArr: [],
      filterArr: [
        // {
        //   expression: "",
        //   key: Math.random(),
        // },
      ],
      currentCol: "",
      mappingParser: {},
      limitOffset: 5,
      timeout: 30,
      request: false,
      visiblePop1: false,
      visiblePop2: false,
      visiblePop3: false,
      allProperties: [],
      dialogVisible: false,
      checkedProperties: [],
      requesting: false
    };
  },
  computed: {
    srules() {
      return {
        s_name: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.transformer.st_input"),
          },
        ],
      };
    },
    docsStyle() {
      return {
        paddingRight: "20px",
        wordBreak: "break-word"
      };
    },
  },

  async created() {
    const plugins = await listParserPlugins();
    this.parseTypes = PARSER_BUILDIN.concat(plugins.map((item) => item.name));
  },

  async mounted() {

    if (this.parserColumns) {
      if (
        this.$store.state.app.currentDBType == "mqtt" ||
        this.$store.state.app.currentDBType == "kafka" ||
        this.$store.state.app.currentDBType == "mongodb"
      ) {
        this.initColumnLists(
          this.parserColumns.filter((item) => item.name != "ts")
        );
      } else {
        this.initColumnLists(this.parserColumns);
      }
    }

    if (
      this.$parent.$parent.$parent.isEditable ||
      (this.$store.state.app.csvParser &&
        Object.hasOwn(this.$store.state.app.csvParser, "parser"))
    ) {
      // 编辑状态
      await this.echoParser(this.$store.state.app.transformerParserData);
    }
    if (this.$store.state.app.csvTransformerParser) {
      //CSV新增
      this.isCSV = true;
      this.msgForm.msgbody = this.$store.state.app.csvTransformerParser.msgBody;
      await this.submitParse();
    }
    if (this.$store.state.app.currentDBType == 'avevaHistorian') {
      this.timeout = 120;
    } 
    await this.getInitStables();
    this.statisticCol();
  },
  methods: {
    // handleChange(visible) {
    //   if (visible) {
    //     setTimeout(() => {
    //       this.$refs.inputRef.focus();
    //     }, 100);
    //   }
    // },
    // handleInput(data) {
    //   this.$refs.inputRef.focus();
    //   console.log('Input value:', data.inputValue);
    // },
    // focus() {

    // },
    handleClickPop(key) {
      switch (key) {
        case '1':
          this.visiblePop1 = !this.visiblePop1
          this.visiblePop2 = false
          this.visiblePop3 = false
          break;
        case '2':
          this.visiblePop1 = false
          this.visiblePop2 = !this.visiblePop2
          this.visiblePop3 = false
          break;
        case '3':
          this.visiblePop1 = false
          this.visiblePop2 = false
          this.visiblePop3 = !this.visiblePop3
          break;
        default:
          break;
      }
      
    },
    getJsonText(data) {
      if (data instanceof Object) {
        this.isjson = true;
        this.$set(this, "jsoneditorcont", data);
        this.jsoneditorcont = data;
      } else {
        this.isjson = false;
      }
      
    },
    statisticCol() {
      this.configuredCount = this.tableData.filter(
        (item) => item["Expression"] != ""
      ).length;
    },
    changeCurrentMapExpr(scope) {
      this.$nextTick(() => {
        this.$set(this.pageTableData[scope.$index], "Expression", "");
        if (this.pageTableData[scope.$index].default != undefined && this.pageTableData[scope.$index].default !== "") {
          this.$set(this.pageTableData[scope.$index], "default", "");
          this.$set(this.pageTableData[scope.$index], "defaultValueError", "");
        }
        if (scope.row.exprname == "generator") {
          this.$set(this.pageTableData[scope.$index], "Expression", "now");
        }
      });
    },
    async getMsgBody() {
      this.$refs.sruleForm.clearValidate();
      this.$parent.$parent.$parent.validateRetrieve();
      let flag = false;
      await this.$nextTick(() => {
        const dom = document.querySelector(".source-ui .left-ui .is-error");
        if (dom) {
          dom.scrollIntoView();
          flag = true;
        }
      });
      if (flag) {
        return;
      }
      this.requesting = true;
      let isSupportType = this.$store.state.app.currentDBType == 'kafka' || this.$store.state.app.currentDBType == 'mqtt' || this.$store.state.app.currentDBType == 'mongodb'
      let dsn = getDsnData(
        this.$parent.$parent.$parent.sourceForm.data,
        this.$parent.$parent.$parent.currentDefinition
      );
      dsn += `&sample_data_limit=${this.limitOffset}`
      if (isSupportType) {
        dsn += `&get_sample_timeout=3`
      }
      let result = await getSampleDataMsgbody(
        this.$store.state.app.currentDBType,
        encodeURIComponent(dsn),
        this.sourceParent.sourceForm.agent,
        this.$store.state.app.supportSQL ? this.timeout : ''
      );
      if (result && Object.hasOwnProperty.call(result,'code')) {
        this.$error(result.message || result.desc)
        if (!isSupportType) {
          this.msgForm.msgbody = '';
        }
        this.requesting = false;
        return
      }
      if (isSupportType) {
        if (result.input.length <= 0) {
          this.$message.warning(this.$t('datasource.transformer.retrieveTip'))
        } else {
          let type = '';
          if (this.$store.state.app.currentDBType == 'kafka') {
            type = 'Kafka';
          } else if (this.$store.state.app.currentDBType == 'mqtt') {
            type = 'MQTT';
          } else if (this.$store.state.app.currentDBType == 'mongodb') {
            type = 'MongoDB';
          }
          this.$message.success(
            type + this.$t('datasource.transformer.retrieveSuccTip').replace('{n}',result.input.length)
          )
        }
        result.input.map(item => {
          this.msgForm.msgbody += item.payload + "\n";
        })
      } else {
        this.msgForm.msgbody = JSON.stringify(result);
      }
      this.requesting = false;
      // mqtt、kafka、mongodb 的从服务器获取数据后，只是追加到示例数据 textarea 中，不触发预览数据
      if (!isSupportType) {
        await this.submitParse();
      }
    },
    clearMsgBody() {
      this.msgForm.msgbody = ''
    },
    validateSubName() {
      let flag = false;
      if (this.$refs.subtb && this.$refs?.subtb[0]) {
        this.$refs?.subtb[0].validate((valid) => {
          if (valid) {
            flag = true;
          } else {
            flag = false;
          }
        });
      } else {
        flag = false;
      }
      return flag;
    },
    showIndentifyResulttb() {
      this.$store.commit("app/SET_RESULTTB_SHOW", true);
      this.$store.commit("app/SET_RESULTTB_TITLE_SHOW", 'parseResTb');
      if (this.$store.state.app.currentDBType == "csv") {
        this.$nextTick(() => {
          if (document.querySelector(".block-title.top")) {
            let dom = document.querySelector(".block-title.top");
            const mainDom = document.querySelector(".main_content");
            let top = dom.offsetTop + mainDom.scrollHeight;
            this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", top);
          }
        });
      }

      this.$store.commit(
        "app/SET_TRANS_RESULT_NAME",
        this.$t("datasource.transformer.identified")
      );
    },
    handleExceed(files, fileList) {
      this.$message.warning(
        `当前限制选择 3 个文件，本次选择了 ${files.length} 个文件，共选择了 ${
          files.length + fileList.length
        } 个文件`
      );
    },
    handleStart() {
      this.request = true;
    },
    handleError() {
      this.request = false;
    },
    handleSuccess(_, file, fileList) {
      const reader = new FileReader();
      const _this = this;
    
      reader.onload = function(e) {
        const contents = e.target.result;
        _this.msgForm.msgbody += contents + "\n";
        _this.request = false;
      };
    
      reader.readAsText(file.raw); // 读取文本文件
    },
    handleSuccessUdt(_, file, fileList) {
      const reader = new FileReader();
      const _this = this;
      this.parseruleForm.expression = "";
      
      reader.onload = function(e) {
        const contents = e.target.result;
        _this.parseruleForm.expression += contents + "\n";
        _this.request = false;
      };
    
      reader.readAsText(file.raw); // 读取文本文件
    },
    beforeRemove(file, fileList) {
      return this.$confirm(`确定移除 ${file.name}？`);
    },
    //获取所有的extract或者split结果
    getAllExtract() {
      this.$refs.extract[0].submitExtract(true);
    },
    filterEmpty(val) {
      if (
        Object.is(val, undefined) | Object.is(val, "") ||
        Object.is(val, null)
        ) {
        return "";
      }
      if (Object.is(val, 0) || Object.is(val, false) || Object.is(val, true) || typeof val == 'object') {
        return val.toString();
      }
      return val;
    },
    async submitParse(name) {
      try {
        
        for (let i = 0; i < this.pageTableData.length; i++) {
          if (this.pageTableData[i].defaultValueError) {
            this.$error(this.pageTableData[i].defaultValueError);
            return;
          }
        }

        if (!this.msgForm.msgbody) {
          Message.warning(this.$t("datasource.transformer.msgbodytip"));
          return;
        }
        let topparser = null;

        if (this.$store.state.app.supportSQL) {
          topparser = JSON.parse(this.msgForm.msgbody);
        } else if (PARSER_BUILDIN.includes(this.parseruleForm.type)) {
          let depthObj = {}
          if (this.parseruleForm.type == 'json' && (this.parseruleForm.depth || this.parseruleForm.depth == 0)) {
            depthObj = {
              'depth': this.parseruleForm.depth
            }
          }
          let expressionObj = {
            [`${this.parseruleForm.type}`]:
              this.parseruleForm.type == "regex"
                ? this.parseruleForm.expression
                : this.parseruleForm.type == "split"
                ? this.$store.state.app.splitExpresList
                : this.parseruleForm.type == "udt"
                ? this.parseruleForm.expression
                : this.parseruleForm.expression
                ? this.parseruleForm.expression
                    .split(";")
                    .toString()
                    .split(",")
                    .map((item) => item.trim())
                : this.parseruleForm.expression
          }
          topparser = {
            parser: {
              parse: {
                [this.$store.state.app.currentDBType == "mqtt"
                  ? "payload"
                  : "value"]: {
                    ...expressionObj,
                    ...depthObj
                },
              },
            },
            input:
              this.$store.state.app.currentDBType == "csv"
                ? this.$store.state.app.csvTransformerParser.inputList
                : [].concat(this.generateInput()),
          };
        } else {
          topparser = {
            parser: {
              parse: {
                [this.$store.state.app.currentDBType == "mqtt"
                  ? "payload"
                  : "value"]: {
                    "plugin_type": this.parseruleForm.type,
                    "plugin_params": this.parseruleForm.expression
                  },
              },
            },
            input:
              this.$store.state.app.currentDBType == "csv"
                ? this.$store.state.app.csvTransformerParser.inputList
                : [].concat(this.generateInput()),
          };
        }
        let checkResult = checkParseData(topparser);
        if (checkResult) {
          this.$message.warning(this.$t(checkResult));
          return;
        }
        this.$store.commit("app/SET_TOP_PARSE", topparser);
        let result = await getParser(topparser);
        if (result.message) {
          this.$error(result.message);
          this.isbreak = true;
          return;
        }
        let transformerColumns = [
          {
            value: "expression",
            label: this.$t("expression"),
            children: this.maptypes.map((item) => {
              return {
                value: item,
                label: item,
              };
            }),
          },
          {
            value: "mapping",
            label: this.$t("mapping"),
            children: result[0].fields.map((item) => {
              return {
                value: item.name,
                label: item.name,
              };
            }),
          },
        ];
        this.$store.commit(
          "app/SET_TRANSFORMER_MAPCOLUMNS",
          transformerColumns
        );
        this.isbreak = false;
        let hiddenCols = [];
        if (this.$store.state.app.currentDBType == "mqtt") {
          hiddenCols = this.mqttDefaultCols;
        } else if (this.$store.state.app.currentDBType == "kafka") {
          hiddenCols = this.kafkaDefaultCols;
        } else if (this.$store.state.app.currentDBType == "mongodb") {
          hiddenCols = this.mongodbDefaultCols;
        }
        let tbdata = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields
              .map((item, index) => {
                return [
                  item.name,
                  this.filterEmpty(data[index]) 
                    ? (Array.isArray(data[index]) ? JSON.stringify(data[index]) : data[index].toString()) 
                    : null,
                ];
              })
              .filter((f) => !hiddenCols.includes(f[0]))
          );
        });
        this.$store.commit("app/SET_TRANS_RESULT_TABLE", tbdata);
        this.$store.commit("app/SET_ACTIVE_COLS", []);
        this.$store.commit("app/SET_RESULT_PAGE", 1);
        this.columnsArr = (
          this.$store.state.app.currentDBType == "csv"
            ? result[0].fields
            : result[0].fields.filter((item) => {
                if (
                  this.$store.state.app.currentDBType == "mqtt" &&
                  !this.mqttDefaultCols.includes(item.name)
                ) {
                  return item;
                } else if (
                  this.$store.state.app.currentDBType == "kafka" &&
                  !this.kafkaDefaultCols.includes(item.name)
                ) {
                  return item;
                } else if (
                  this.$store.state.app.currentDBType == "mongodb" &&
                  !this.mongodbDefaultCols.includes(item.name)
                ) {
                  return item;
                } else if (this.$store.state.app.supportSQL) {
                  return item;
                }
              })
        ).map((val) => {
          let finalVal = tbdata.map((item) => {
            return item[val.name];
          });

          return {
            description: val.name,
            name: val.name,
            show: true,
            type: !val.arrow_type?.List ? "string" : "array",
            localType: val.type,
            value:
              this.$t("datasource.transformer.sampleval") +
              ":" +
              (finalVal.join("") ? finalVal.join(" ; ") : ""),
          };
        });
        if (!this.$store.state.app.transresultname) {
          this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
        }
        this.$store.commit("app/SET_STB_DEFAULT_COLUMNS",this.columnsArr)

        // 删除 extractArr 中没有包含 columnsArr 中拆分的字段
        this.handelExtractArr(this.columnsArr,this.extractArr)
        this.showIndentifyResulttb();
      } catch (error) {
        console.log(error);
      }
    },
    handelExtractArr(columnsArr,extractArr) {
      let names = columnsArr.map(obj => obj.name);
      // 过滤 extractArr，移除不在 names 中的对象
      let arr = extractArr.filter(obj => names.includes(obj.columnname));
      this.extractArr = arr;
    },
    setPageTableData() {
      this.$set(
        this,
        "pageTableData",
        this.tableData.slice(
          (this.currentPage - 1) * this.pageSize,
          this.currentPage * this.pageSize
        )
      );
    },
    onDefaultValueInput(name, val, range) {
      if (val === undefined || val.trim() === "") {
        this.setDefaultValueError(name, "");
        return;
      }

      if (range[2] <= 20) {
        // 整数
        if (val.indexOf(".") >= 0 || isNaN(val) || val.length > range[2]) {
          this.alertDataRange(name, val, range);
          return;
        }
        let ival;
        if (range[2] < 20) {
          ival = parseInt(val);
        } else {
          ival = eval(val + "n");
        }
        if (ival < range[0] || ival > range[1]) {
          this.alertDataRange(name, val, range);
          return;
        }
      } else {
        // 浮点数
        if (isNaN(val) || val.length > range[2]) {
          this.alertDataRange(name, val, range);
          return;
        }
        let fval = parseFloat(val);
        if (fval < range[0] || fval > range[1]) {
          this.alertDataRange(name, val, range);
          return;
        }
      }

      this.setDefaultValueError(name, "");
    },
    alertDataRange(name, val, range) {
      let dataRangeInputTip = this.$t("datasource.transformer.dataRangeInputTip");
      dataRangeInputTip = dataRangeInputTip.replace("{min}", range[0]).replace("{max}", range[1]);
      this.setDefaultValueError(name, dataRangeInputTip);
    },

    setDefaultValueError(name, errorMsg) {
      this.pageTableData.forEach((item) => {
        if (item.Name === name) {
          this.$set(item, "defaultValueError", errorMsg);
        }
      });
    },

    handleCurrentChange(val) {
      this.currentPage = val;
      this.pageTableData.splice(0, Infinity);
      this.setPageTableData();
    },

    //编辑回显数据--编辑状态不自动显示result table
    async echoParser(value) {
      if (this.$store.state.app.supportSQL) {
        let dsn = this.$store.state.app.historiandsn;
        dsn += `&sample_data_limit=${this.limitOffset}`
        let result = await getSampleDataMsgbody(
          this.$store.state.app.currentDBType,
          encodeURIComponent(dsn),
          this.sourceParent.sourceForm.agent
        );
        if (result && Object.hasOwnProperty.call(result,'code')) {
          this.$error(result.message)
          this.msgForm.msgbody = '';
          return
        }
        this.msgForm.msgbody = JSON.stringify(result);
        value = this.$store.state.app.historianechodata;
      } else {
        let csvechoTransData = null;
        this.currentPage = value?.format?.currentPage;
        if (this.$store.state.app.currentDBType == "csv") {
          this.isCSV = true;
          csvechoTransData = this.$store.state.app.csvTransformerParser;
          let columns = csvechoTransData.columns.map((item) => {
            return {
              description: item,
              name: item,
              type: "varchar",
              value: "",
            };
          });
          this.initColumnLists(columns);
        }

        this.msgForm.msgbody =
          this.$store.state.app.currentDBType == "mqtt"
            ? value.input.map((item) => item.payload).join(" ")
            : this.isCSV
            ? csvechoTransData.msgBody
            : value.input.map((item) => item.value).join(" ");

        let tagKey = "";
        switch (this.$store.state.app.currentDBType) {
          case "mqtt":
            tagKey = "payload";
            break;
          default:
            tagKey = "value";
            break;
        }
        let keys = Object.keys(value.parser.parse[tagKey])
        if (keys.includes('plugin_type')) {
          this.parseruleForm.type = value.parser.parse[tagKey]['plugin_type'];
          this.parseruleForm.expression = value.parser.parse[tagKey]['plugin_params'];
        } else {
          this.parseruleForm.type = keys.filter(item => item != 'depth').toString(); 

          if (this.parseruleForm.type == 'json') {
            this.parseruleForm.depth = value.parser.parse[tagKey]['depth']
          }
          this.parseruleForm.expression = value.parser.parse[tagKey][this.parseruleForm.type].toString();
        }

        // this.parseruleForm.expression = 
        //   this.parseruleForm.type == "regex"
        //     ? Object.values(value.parser.parse[tagKey]).toString()
        //     : Object.values(value.parser.parse[tagKey])
        //         .toString()
                // .replace(",", ";");
      }

      await this.submitParse();

      let identifiedColObj = value?.parser.mutate.filter((item) => {
        if (Object.keys(item).toString() == "extract") {
          return item;
        }
      })[0];

      if (identifiedColObj?.extract) {
        Object.entries(identifiedColObj.extract).forEach((item) => {
          let ind = this.columnsArr.findIndex((col) => col.name == item[0]);
          let obj = {
            columnname: item[0],
            expression: Object.values(item[1]).flat(1).join(";"),
            type: Object.keys(item[1]).toString(),
            columns: this.columnsArr,
            key: Math.random(),
          };
          if (ind > -1) {
            this.$set(this.columnsArr[ind], "show", false);
          }
          if (Object.keys(item[1]).toString() == "split") {
            obj["splitParams"] = Object.keys(item[1]["split"])
              .map((k) => {
                return {
                  [k]: String(item[1]["split"][k]),
                };
              })
              .reduce((a, b) => {
                a[Object.keys(b).toString()] = String(
                  b[Object.keys(b).toString()]
                );
                return a;
              }, {});
          }

          if (this.columnsArr.length > 0) {
            this.extractArr.push(obj);
          }
        });
        this.$store.commit(
          "app/SET_EXTRACT_PARSE_DATA",
          value.parser.mutate.extract
        );
      }

      let echoMapData = [];
      let isincludeFilter = false;
      value.parser.mutate.forEach((item) => {
        if (Object.keys(item).toString() == "filter") {
          isincludeFilter = true;
          let obj = {
            expression: item.filter,
            key: Math.random(),
          };
          this.filterArr.splice(0, this.filterArr.length, obj);
        }
        if (Object.keys(item).toString() == "map") {
          echoMapData = Object.entries(item["map"]).map((val) => {
            let expreKey = Object.keys(val[1]).filter((key) => key != "as")[0];
            
            return {
              columnname: val[0],
              type: expreKey,
              expression: val[1][expreKey],
              default: val[1]["default"] || "",
              joinwith: val[1]["with"] || "",
              datatype: val[1]["as"],
            };
          });
        }
      });
      this.$store.commit("app/SET_ECHO_MAP_DATA", {
        model: value.parser.model,
        tableData: echoMapData,
      });
      this.$nextTick(async () => {
        if (this.$refs.extract && this.$refs.extract.length > 0) {
          let newarr = [];
          for (let i = 0; i < this.$refs.extract.length; i++) {
            newarr.push(this.$refs.extract[i].submitExtract());
          }
          await this.$refs.extract[0].submitExtract(true);
          await Promise.all(newarr);
        }
        if (isincludeFilter) {
          await this.$refs.filter[0].submitFilter();
        }
        this.sruleForm.s_name = value.parser.model.using;
        // this.subrule.subname = value.parser.model.name;
        await this.getSTbaleList(true);
        await this.echoFetchMap();
        if (this.$store.state.app.currentDBType !== 'csv' && !this.$store.state.app.supportSQL) {
          await this.selectJson();
        }
        this.$store.commit("app/SET_RESULTTB_SHOW", false);
        this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
      });
    },
    clearTargetTBWhenDelete() {
      // 数据库为空
      if (!this.sourceParent.sourceForm.targetDB) {
        //  || !this.stableLists.find((v) => v === this.sruleForm.s_name)
        this.clearStbMapping()
      } 
    },
    clearStbMapping() {
      this.sruleForm.s_name = ""
      this.tableData = [];
      this.params_tags = [];
      this.setPageTableData();
    },
    //初始化列下拉框数据，适用于新增和编辑，拷贝
    initColumnLists(columns) {
      this.$set(
        this,
        "indentifiedColumns",
        columns.map((item) => {
          return {
            ...item,
            show: true,
          };
        })
      );
      let finalCol = [];
      switch (this.$store.state.app.currentDBType) {
        case "csv":
          finalCol = columns
            .filter((val) => ["varchar", "nchar"].includes(val.type))
            .map((item) => {
              return {
                ...item,
                show: true,
              };
            });
          break;
        case "mqtt":
          finalCol = columns
            .filter((val) => ["payload"].includes(val.name))
            .map((item) => {
              return {
                ...item,
                show: true,
              };
            });
          break;
        case "kafka":
          finalCol = columns
            .filter((val) => ["value"].includes(val.name))
            .map((item) => {
              return {
                ...item,
                show: true,
              };
            });
          break;
        case "mongodb":
          finalCol = columns
            .filter((val) => ["value"].includes(val.name))
            .map((item) => {
              return {
                ...item,
                show: true,
              };
            });
          break;
      }
    },
    validateTransform() {
      let msgflag = this.validateMsgBody();
      let stableflag = this.validateTargetStb();
      if (msgflag && stableflag) {
        return true;
      }

      return false;
    },
    //messagebody非空验证触发
    validateMsgBody() {
      let flag = false;
      this.$refs.msgForm?.validate((valid) => {
        if (valid) {
          flag = true;
        } else {
          flag = false;
        }
      });
      return flag;
    },
    validateTargetStb() {
      let flag = false;
      this.$refs.sruleForm.validate((valid) => {
        if (valid) {
          flag = true;
        } else {
          flag = false;
        }
      });
      return flag;
    },
    //计算mapping的结果
    async caculateMappingResult() {
      if (!this.validateTransform()) {
        this.isbreak = true;
        return;
      }
      this.$nextTick(() => {
        document
          .querySelector(".common-transformer .el-form-item__error")
          ?.scrollIntoView();
        return;
      });
      if (!this.msgForm.msgbody) {
        this.isbreak = true;
        return false;
      }

      if (this.tableData && !this.tableData[0]?.["Expression"] && this.sruleForm.s_name) {
        Message.warning(this.$t("datasource.transformer.tablenametip")); 
        this.isbreak = true;
        return false;
      }
      this.isbreak = false;
      let tags = [];
      let columns = [];
      let commonColumns = [];
      let mutates = [];
      let mutateMap = {};
  
      this.tableData.forEach((item) => {
        // 主键列不能为空
        if (item["PrimaryKey"] && !item["Expression"]) {
          Message.closeAll();
          Message.warning(this.$t("datasource.transformer.mappingvaildtip"));
          this.isbreak = true;
        }
        // 不支持 VARBINARY & GEOMETRY
        if ((item["Type"] == "VARBINARY" || item["Type"] == "GEOMETRY") && item["Expression"]) {
          Message.closeAll();
          Message.warning(this.$t("datasource.transformer.nonsupportTypetip",['VARBINARY/GEOMETRY']))
          this.isbreak = true;
        }
        if (item["Expression"]) {
          if (
            this.params_columns.includes(item["Name"])
          ) {
            columns.push(item["Name"]);
          }
          if (this.params_tags.includes(item["Name"])) {
            tags.push(item["Name"]);
          }
          if (!item["PrimaryKey"] && this.params_columns.includes(item["Name"])) {
            commonColumns.push(item["Name"])
          }
          let key = item.exprname == "mapping" ? "cast" : item.exprname; //此处处理了编辑回显
          if (item["Type"] !== "Tablename") {
            //排除第一行的tablename
            let expreitem = {
              [`${key}`]: ["sum", "join"].includes(key)
                ? item["Expression"]
                : item["Expression"].toString().trim(),
              as: item["Type"],
            };
            if (key == "join") {
              expreitem["with"] = item.joinwith;
            }
            if (item.defaultValueError) {
              this.isbreak = true;
              this.$error(this.$t("data.fields") + "[" + item.Name + "]," +  item.defaultValueError);
            }
            if (item.exprname == "mapping" && this.params_columns.includes(item["Name"])) {
              if (item.default) {
                if (item.dataType === "TIMESTAMP") {
                  expreitem["default"] = item.default + "";
                } else {
                  expreitem["default"] = item.default;
                }
              }
            }
            mutates.push({
              [`${item["Name"]}`]: expreitem,
            });
          }
        }
      });
      if (this.isbreak) return;

      mutates.forEach((item) => {
        Object.assign(mutateMap, item);
      });

      let parserData = {
        parser: {
          parse: this.$store.state.app.topParse.parser.parse,
          model: {
            name: this.tableData[0]["Expression"],
            using: this.sruleForm.s_name,
            tags: tags,
            columns: columns,
          },
          mutate: this.$store.state.app.transformerFilterParseData
            ? this.$store.state.app.transformExtractParseData
              ? []
                  .concat(this.$store.state.app.transformExtractParseData)
                  .concat({
                    filter: Object.values(
                      this.$store.state.app.transformerFilterParseData
                    ).toString(),
                  })
                  .concat({
                    map: mutateMap,
                  })
              : []
                  .concat({
                    filter: Object.values(
                      this.$store.state.app.transformerFilterParseData
                    ).toString(),
                  })
                  .concat({
                    map: mutateMap,
                  })
            : this.$store.state.app.transformExtractParseData
            ? []
                .concat(this.$store.state.app.transformExtractParseData)
                .concat({
                  map: mutateMap,
                })
            : [].concat({
                map: mutateMap,
              }),
        },
        input: this.isCSV
          ? this.$store.state.app.csvTransformerParser.inputList
          : this.$store.state.app.supportSQL
          ? this.$store.state.app.topParse.input
          : [].concat(this.generateInput()),
        format: {
          pageCount: this.pageCount,
          pageSize: this.pageSize,
          currentPage: this.currentPage,
        },
      };

      // 至少必须配置一个tag和一个column 
      if (tags.length == 0 || commonColumns.length == 0) {
        Message.closeAll();
        Message.warning(this.$t("datasource.transformer.mappingvaildColtip"));
        this.isbreak = true;
        return;
      }
      this.isbreak = false;
      this.mappingParser = parserData;
      await this.getParserData(parserData);
    },
    //设置extract的name
    setExtractName(index, name) {
      this.$set(this.extractArr[index], "columnname", name);
    },
    //给filter赋值
    changeFilter(key, value) {
      let index = this.filterArr.findIndex((val) => val.key == key);
      this.$set(this.filterArr[index], "expression", value);
    },
    //extract的expression赋值
    changeExtractExpr(colname, value) {
      let index = this.extractArr.findIndex(
        (item) => item.columnname == colname
      );
      this.$set(this.extractArr[index], "expression", value);
    },
    //获取transformer的所有参数
    async getTransformerParams() {
      await this.caculateMappingResult();
      if (this.isbreak) return;
      let extractObj = {};
      this.extractArr.forEach((item) => {
        extractObj[item.columnname] = {
          [`${item.type}`]:
            item.type == "regex" || item.type == "join"
              ? item.expression
              : item.type == "split"
              ? this.$store.state.app.splitExpresList
              : item.expression
              ? item.expression.split(";").map((item) => item.trim())
              : item.expression,
        };
      });

      let parserData = {
        parser: {
          parse: this.$store.state.app.topParse.parser.parse,
          model: this.mappingParser.parser.model,
          mutate: this.mappingParser.parser.mutate,
        },

        input: this.isCSV
          ? this.$store.state.app.csvTransformerParser.inputList
          : this.$store.state.app.supportSQL
          ? this.$store.state.app.topParse.input
          : [].concat(this.generateInput()),
        format: {
          pageCount: this.pageCount,
          pageSize: this.pageSize,
          currentPage: this.currentPage,
        },
      };
    
      this.$store.commit("app/SET_TRANS_FULL_PARAMS", parserData);
      // this.$emit("getTransformerParams", parserData);
    },
    changeColumnStatus(index, name) {
      //选中的列不能再选中
      let ind = this.columnsArr.findIndex((item) => item.name == name);
      this.$set(this.columnsArr[ind], "show", false);
      this.extractAddStatus = this.columnsArr.every((item) => !item.show);
      this.$set(this.extractArr[index], "columnname", name);
      this.$set(this.extractArr[index], "value_type", this.columnsArr[ind].type);
    },
    async getParserData(data) {
      try {
        let checkResult = checkParseData(data);
        if (checkResult) {
          this.$message.warning(this.$t(checkResult));
          return;
        }
        let result = await getParser(data);
        if (result.message) {
          this.$error(result.message);
          this.isbreak = true;
          return;
        }
        this.isbreak = false;
        let outputColumns = result[0].fields.map((item) => item.name);
        let outputTBData = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, this.filterEmpty(data[index])];
            })
          );
        });
        let overlapColumns = [];
        this.tableData
          .map((val) => val["Name"])
          .forEach((item) => {
            if (outputColumns.includes(item)) {
              overlapColumns.push(item);
            }
          });
        if (outputColumns.includes("__tbname__")) {
          let index = this.tableData.findIndex(
            (item) => item["Type"] == "Tablename"
          );
          overlapColumns.push(this.tableData[index]["Name"]);
        }
        
        // 预览映射结果table数据
        let resultTableData = outputTBData.map(item => {
          item.SubTableName = item['__tbname__'];
          const { __using__, __tbname__, ...rest } = item;
          return rest;
        });
        this.$store.commit('app/SET_RESULTTB_SHOW',true);
        this.$store.commit("app/SET_RESULTTB_TITLE_SHOW", 'mappingResTb');
        this.$store.commit("app/SET_TRANS_RESULT_TABLE", resultTableData);
        this.$store.commit("app/SET_TRANS_RESULT_NAME",'mappping');

        this.setPageTableData();
      } catch (error) {
        console.log(error);
      }
    },
    
    //输出input结果
    generateInput() {
      let demo_list;
      try {
        if (this.parseruleForm.type == "regex") {
          demo_list = this.msgForm.msgbody.split(/\n+/);
        } else {
          demo_list = getExampleList(this.msgForm.msgbody);
        }
      } catch (err) {
        if (err.lineNumber > 0) {
          this.$error(this.$t("datasource.transformer.jsonDemoError").replace("{0}", err.lineNumber).replace("{1}", err.message));
        } else {
          this.$error(this.$t(err));
        }
      }

      let inputList = demo_list.map((msg) => {
        let inputobj = {};
        this.indentifiedColumns.forEach((item) => {
          if (msg) {
            if (this.$store.state.app.currentDBType == "mqtt") {
              if (item.name == "payload") {
                inputobj["payload"] = msg;
              } else {
                inputobj[item.name] =
                  item.type == "timestamp"
                    ? "" //parsinginZone(new Date())
                    : item.name;
              }
            } else if (this.$store.state.app.currentDBType == "kafka") {
              if (item.name == "value") {
                inputobj["value"] = msg;
              } else {
                inputobj[item.name] =
                  item.type == "timestamp"
                    ? "" //parsinginZone(new Date())
                    : item.name;
              }
            } else if (this.$store.state.app.currentDBType == "mongodb") {
              if (item.name == "value") {
                inputobj["value"] = msg;
              } else {
                inputobj[item.name] =
                  item.type == "timestamp"
                    ? "" //parsinginZone(new Date())
                    : item.name;
              }
            }
          }
        });
        return inputobj;
      });
      return inputList.filter(v => JSON.stringify(v) !== '{}');
    },
    submitSuper(data) {
      if (!this.msgForm.msgbody) {
        this.$error(this.$t("datasource.transformer.msgbodytip"));
        return;
      }
      if (!this.tableData[0]["Expression"]) {
        this.$error(this.$t("datasource.transformer.tablenametip"));
        return;
      }
      this.currentCol = data["Name"];

      let parserData = {
        parser: {
          parse: this.$store.state.app.transformExtractParseData,
          model: {
            name: this.tableData[0]["Expression"],
            using: this.sruleForm.s_name,
            tags: this.params_tags.includes(data["Name"])
              ? [].concat(data["Name"])
              : [],
            columns: this.params_columns.includes(data["Name"])
              ? [].concat(data["Name"])
              : [],
          },
          mutate: [
            {
              map: {
                [`${data["Name"]}`]: {
                  [`${data.maptype}`]: data["Expression"].split(";").toString(),
                },
              },
            },
          ],
        },
        input: [].concat(this.generateInput()),
      };
      
      this.getParserData(parserData);
    },
    closeDialog() {
      this.dialogForm.st_name = "";
      this.showCreateDialog = false;
    },
    //创建或者查询
    async createST() {
      this.$refs.createstb.$refs.form.validate(async (valid) => {
        if (!valid) return false;
        if (valid) {
          try {
            const { ts_field_name, tags, columns } =
              this.$refs.createstb.stable_form;
            // if (!ts_field_name) {
            //   return Message.warning(
            //     this.$t("dataIn.enterTip") + " " + this.$t("data.columnNameTip")
            //   );
            // }
            for (let i = 0; i < columns.length; i++) {
              const element = columns[i];
              if (!element.field) {
                return Message.warning(
                  this.$t("dataIn.enterTip") +
                    " " +
                    this.$t("data.columnNameTip")
                );
              }
            }
            for (let i = 0; i < tags.length; i++) {
              const element = tags[i];
              if (!element.field) {
                return Message.warning(
                  this.$t("dataIn.enterTip") + " " + this.$t("data.tagNameTip")
                );
              }
            }
            if (!this.version_gt_3300) {
              this.$refs.createstb.stable_form.columns = this.$refs.createstb.stable_form.columns.map((item) => {
                return {
                  ...item,
                  encode: '',
                  compress: '',
                  level: ''
                }
              });
            }
            
            let payload = {
              selected_db: this.$store.state.app.currentDBName,
              stable_form: this.$refs.createstb.stable_form,
            };
            let result = await createStableReq(payload);
            if (result?.desc) {
              this.$error(this.$t(result.desc));
              return;
            }
            Message.success(this.$t("operateSucc"));
            await this.getInitStables();
            this.sruleForm.s_name = this.$refs.createstb.stable_form.name;
            this.getSTbaleList(false);
            this.closeDialog();
          } catch (error) {
            error.desc ? this.$error(error.desc) : "";
            console.log(error);
          }
        }
      });
    },
    //获取初始化的stables
    async getInitStables() {
      try {
        if (!this.$store.state.app.currentDBName) return;
        let result = await sendSQLReq(
          `show  \`${this.$store.state.app.currentDBName}\`.stables `
        );
        this.$set(this, "stableLists", Array.from(result.data).flat(1));

        if (this.sourceParent.isEditable) {
          this.clearTargetTBWhenDelete();
        }
      } catch (error) {
        console.log(error);
      }
    },
    createStable() {
      if (!this.$store.state.app.currentDBName) {
        this.$store.commit("app/SET_CREATESTWITHOUT_DB", 1);
        return;
      } else {
        this.$store.commit("app/SET_CREATESTWITHOUT_DB", 2);
      }
      // 获取最新的拆分后的列
      if (this.$refs.extract && this.$refs.extract.length > 0) {
        this.$refs.extract[this.$refs.extract.length - 1].submitExtract(true);
      }

      this.showCreateDialog = true;
    },
    //回显数据调用mapping接口
    echoFetchMap() {
      let echoData = this.$store.state.app.transformEchoMapData;
      if (echoData) {
        //编辑回显
        this.tableData.map((item) => {
          if (
            echoData.tableData.map((v) => v.columnname).includes(item["Name"])
          ) {
            let idx = echoData.tableData.findIndex(
              (val) => val.columnname == item["Name"]
            );
            item.maptype = []
              .concat(item.maptype[0])
              .concat(
                echoData.tableData[idx].type == "cast"
                  ? echoData.tableData[idx].expression
                  : echoData.tableData[idx].type
              );
            item.exprname =
              echoData.tableData[idx].type == "cast"
                ? "mapping"
                : echoData.tableData[idx].type;
            item["Expression"] = ["sum", "join"].includes(item.exprname)
              ? echoData.tableData[idx].expression
              : echoData.tableData[idx].expression.toString();

            if (echoData.tableData[idx].default) {
              this.$set(item, "default", echoData.tableData[idx].default);
            }
            if (echoData.tableData[idx].joinwith) {
              this.$set(item, "joinwith", echoData.tableData[idx].joinwith);
            }
          }
          return item;
        });
        if (this.tableData[0]) {
          this.$set(
            this.tableData[0],
            "Expression",
            echoData.model.name.toString()
          );
        }
        this.clearTargetTBWhenDelete();
        this.caculateMappingResult();
      }
    },
    async getSTbaleList(isEcho) {
      try {
        this.currentPage = 1;
        let res = await sendSQLReq(
          `desc \`${this.$store.state.app.currentDBName}\`.\`${this.sruleForm.s_name}\``
        );
        let precision = await sendSQLReq(`
        select \`precision\` from information_schema.ins_databases where name = '${this.$store.state.app.currentDBName}'
        `);
        if (res.desc) {
          this.$error(res.desc);
          return;
        }
  
        if (this.$store.state.app.transformerMapCloumns) {
          this.$set(
            this,
            "options",
            this.$store.state.app.transformerMapCloumns
          );
          this.$set(
            this,
            "mappingcolumns",
            this.$store.state.app.transformerMapCloumns
              .filter((item) => item.value == "mapping")[0]
              .children.filter((val, index) => {
                if (
                  this.$store.state.app.currentDBType == "mqtt" &&
                  !this.mqttDefaultCols.includes(val.value)
                ) {
                  return val;
                } else if (
                  this.$store.state.app.currentDBType == "kafka" &&
                  !this.kafkaDefaultCols.includes(val.value)
                ) {
                  return val;
                } else if (
                  this.$store.state.app.currentDBType == "mongodb" &&
                  !this.mongodbDefaultCols.includes(val.value)
                ) {
                  return val;
                }  else if (
                  this.$store.state.app.supportSQL
                ) {
                  return val;
                } else {
                  return val;
                }
              })
          );
        }
        let defaultmap = this.options
          .filter((item) => item.value == "mapping")[0]
          ?.children.map((label) => label.label) || [];
        this.params_columns.splice(0, this.params_columns.length - 1);
        this.params_tags.splice(0, this.params_tags.length - 1);
        this.pageCount = res.data.length + 1;
        this.tableData = res.data.map((val, index) => {
          const tableRow = { Name: val[0], exprname: "mapping" };
          if (val[3] !== 'TAG' && index > 0) {
            this.params_columns.push(val[0]); //存储非主键列
            const dataRange = getDataRange(val[1]);
            dataRange && (tableRow.dataRange = dataRange);
            tableRow.dataType = val[1];
          }
          if (val.includes("TAG")) {
            this.params_tags.push(val[0]);
          }
          let equalindex = defaultmap.findIndex(
            (item) => item.toLowerCase() == val[0].toLowerCase()
          );

          tableRow.Type =
              val[1] == "TIMESTAMP"
                ? val[1] + "(" + precision.data[0][0] + ")"
                : val[1];
          tableRow.maptype =
              equalindex > -1
                ? ["mapping", `${defaultmap[equalindex]}`]
                : ["expression", "value"];
          tableRow.Expression = (equalindex > -1 && !isEcho) ? defaultmap[equalindex] : "";
          tableRow.PrimaryKey = val[3] == "PRIMARY KEY" || (val[1] == "TIMESTAMP" && !index)
          
          return tableRow;
        });

        this.tableData.unshift({
          Name: "SubTableName", //this.sruleForm.s_name,
          Type: "Tablename",
          exprname: "mapping",
          maptype: ["expression", "string"],
          Expression: "",
        });
        this.params_columns.unshift(res.data[0][0]);
        this.setPageTableData();
      } catch (error) {
        console.log(error);
      }
    },

    //新增extract
    addNewExtract() {
      this.extractArr.push({
        columns: this.columnsArr,
        columnname: "",
        expression: "",
        type: "",
        key: Math.random(),
        splitParams: {
          sep: "",
          n: "",
          names: "",
        },
      });
    },
    //新增filter
    addNewFilter() {
      this.filterArr.push({
        expression: "",
        key: Math.random(),
      });
    },
    //删除filter
    deleteFilter(key) {
      this.$confirm(
        this.$t("datasource.deletetip"),
        this.$t("datasource.warning"),
        {
          confirmButtonText: this.$t("datasource.ok"),
          cancelButtonText: this.$t("datasource.cancel"),
          type: "warning",
        }
      ).then(() => {
        let ind = this.filterArr.findIndex((val) => val.key == key);
        this.filterArr.splice(ind, 1);
        this.$store.commit("app/SET_FILTER_PARSE_DATA", null);

        if (this.$refs.extract && this.$refs.extract.length > 0) {
          this.$refs.extract[this.$refs.extract.length - 1].submitExtract(true);
        } else {
          this.submitParse();
        }
      });
    },
    deleteExtract(index, name) {

      if (!name) {
        // 没有设置name的情况下，直接删除
        this.extractArr.splice(index, 1);
        return;
      }

      this.$confirm(
        this.$t("datasource.deletetip"),
        this.$t("datasource.warning"),
        {
          confirmButtonText: this.$t("datasource.ok"),
          cancelButtonText: this.$t("datasource.cancel"),
          type: "warning",
        }
      ).then(() => {
        if (this.$store.state.app.transresultname == name) {
          // 删除提取拆分列时 transresultname 更改为当前删除name,预览表格才能展示正确
          this.$store.commit("app/SET_TRANS_RESULT_NAME", name);
        }
        let oldextract = this.$store.state.app.transformExtractParseData;

        if (oldextract && Object.keys(oldextract.extract).includes(name)) {
          delete oldextract.extract[name];
        }

        let ind = this.extractArr.findIndex(
          (item) => item.columnname == name
        );
        this.extractArr.splice(ind, 1);
        let restoreIndex = this.columnsArr.findIndex(
          (item) => item.name == name
        );
        this.$set(this.columnsArr[restoreIndex], "show", true);
        
        if (this.extractArr.length > 0) {
          if (this.filterArr.lenght > 0 && this.$refs.filter[0].isexecuted) {
            this.$refs.filter[0].submit();
          } else {
            this.$refs.extract[0].submitExtract();
            this.$refs.extract[0].submitExtract(true);
          }
        } else {
          this.$store.commit("app/SET_EXTRACT_PARSE_DATA", null);
          this.filterArr.splice(0, 1);
          this.$store.commit("app/SET_FILTER_PARSE_DATA", null);
          if (this.filterArr.lenght > 0 && this.$refs.filter[0].isexecuted) {
            this.$refs.filter[0].submit();
          } else {
            this.submitParse(name);
          }
        }
      });
    },
    //-----------------------处理csv部分
    //组合csv的extract
    formatCSVExtract(columns) {
      this.columnsArr = columns.map((item) => {
        return {
          description: item,
          name: item,
          show: true,
          type: "varchar",
          value: "",
        };
      });
      this.indentifiedColumns = columns.map((item) => {
        return {
          description: item,
          name: item,
          show: true,
          type: "varchar",
          value: "",
        };
      });
    },
    handleLimit(val) {
      this.$store.commit("app/SET_LIMIT_OFFSET", val);
    },
    updateData(data) {
      this.parseruleForm.expression = data
    },
    selectJson() {
      
      try {
        if (this.parseruleForm.type == 'json') {
          this.allProperties = extractAllProperties(this.msgForm.msgbody, this.parseruleForm.depth)
        }
      } catch (err) {
        if (err.lineNumber > 0) {
          this.$error(this.$t("datasource.transformer.jsonDemoError").replace("{0}", err.lineNumber).replace("{1}", err.message));
        } else {
          this.$error(this.$t(err));
        }
      }
      
      if (this.parseruleForm.expression && this.parseruleForm.type == "json") {
        // 回显逻辑
        let firstSplitArr = this.parseruleForm.expression.split(',')
        let checkedKey = []
        let checkedObj = {}
        firstSplitArr.map(item => {
          let splitArr = item.split('=')
          checkedKey.push(splitArr[0])
          checkedObj[splitArr[0]]= splitArr[1] 
        })

        this.allProperties = this.allProperties.map((item,index) => {
          return  {
            defaultValue: item,
            rename: checkedObj[item] ? checkedObj[item] : checkedKey.includes(item) ? '':  this.handleRename(item),
            checked: checkedKey.includes(item)
          }
        })
      } else {
        this.allProperties = this.allProperties.map((item,index) => {
          return  {
            defaultValue: item,
            rename: this.handleRename(item),
            checked: false
          }
        })
      }
    },
    handleCheckedProperties() {
      let result = []
      this.allProperties.map(item => {
        if (item.checked) {
          item.rename ? result.push(`${item.defaultValue}=${item.rename}`) : result.push(item.defaultValue)
        }
      })
      this.parseruleForm.expression = result?.join(',')
      this.dialogVisible = false
      console.log('this.checkedProperties',this.allProperties);
    },
    handleRename(value) {
      return value.replaceAll("\"][\"", '_').replace("$[\"", "").replace("\"]", "");
    },
    handleTypeChange() {
      this.parseruleForm.expression = ""
      this.parseruleForm.depth = undefined
    }
  },
  watch: {
    tableData: {
      deep: true,
      handler(val) {
        this.statisticCol();
      },
    },
    "$i18n.locale": {
      deep: true,
      handler(val) {
        this.$nextTick(() => {
          this.$refs.sruleForm.clearValidate();
          if (this.$refs.subtb) this.$refs.subtb[0]?.clearValidate();
        });
      },
    },
    //csv需要单独处理
    "$store.state.app.csvTransformerParser": {
      deep: true,
      handler(val) {
        if (val) {
          this.isCSV = true;
          this.msgForm.msgbody = val.msgBody;
          this.formatCSVExtract(val.columns);
        }
      },
    },
    "$store.state.app.transformerMapCloumns": {
      deep: true,
      handler(val) {
        this.$set(this, "options", val);
        this.$set(
          this,
          "mappingcolumns",
          val.filter((item) => item.value == "mapping")[0].children
        );
        let newmappings = this.mappingcolumns.map((item) => item.label);
        this.tableData.map((item) => {
          if (item.exprname == "mapping" && item["Type"] != "Tablename") {
            if (!newmappings.includes(item["Expression"])) {
              item["Expression"] = "";
            }
            return item;
          }
        });
      },
    },

    "$store.state.app.currentDBName": {
      deep: true,
      handler(val) {
        this.getInitStables();
      },
    },
    parserColumns: {
      deep: true,
      handler(val) {
        if (
          this.$store.state.app.currentDBType == "mqtt" ||
          this.$store.state.app.currentDBType == "kafka" ||
          this.$store.state.app.currentDBType == "mongodb"
        ) {
          this.initColumnLists(val.filter((item) => item.name != "ts"));
        } else {
          this.initColumnLists(val);
        }
      },
    },
  },
};
</script>
<style lang="scss" scoped>
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
.msg_sec {
  margin-bottom: 25px;
}
::v-deep i {
  font-size: 16px;
}
::v-deep .btn-icon-small i {
  font-size: 12px;
}

.mapping {
  font-size: 16px;
  font-weight: 600;
  color: #4259ce;
  margin-bottom: 15px;
}
.block-title {
  margin-top: 15px;
  margin-bottom: 10px !important;
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
  margin-top: 15px;
  margin-bottom: 20px;
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  column-gap: 15px;
  row-gap: 15px;
  max-height: 200px;
  overflow-y: auto;
  li {
    color: #4259ce;
    background: #ecf2fe;
    border-radius: 14px;
    border: 1px solid #f6f8fa;
    text-align: center;
  }
  .col.origin {
    background: #409eff;
    color: #fff;
  }
}

.extract, .filter {
  .el-button {
    width: 100%;
  }
  ::v-deep .el-input {
    margin-left: 0px !important;
  }
}
.table-title {
  display: flex;
  white-space: nowrap;
  align-items: center;
  .title {
    display: flex;
    align-items: center;
    flex: 1;
    .el-form-item {
      margin-bottom: 0px;
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
      width: 100px; margin-right: 5px;
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
      color: #ff4949;
      line-height: 1;
      margin-top: 5px;
      font-size: 12px;
      text-align: right;
    }
  }
  ::v-deep {
    .el-table {
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
    .cell.el-tooltip {
      // height: 40px;
      padding-right: 20px;
    }
    .el-form-item {
      margin-bottom: 0px;
      margin-right: 10px;
    }
  }
}
.payload-upload {
  .el-select {
    width: 100%;
  }
}
.upload-demo {
  display: flex;
  align-items: baseline;
  flex: 1;
  ::v-deep .el-upload {
    width: 100%;
    .el-button {
      width: 100%;
    }
  }
}
.buttons {
  display: flex;
  justify-content: center;
  align-items: center;
  .el-button {
    width: 60px;
  }
}
.transdescription {
  color: $color-description;
  margin-bottom: 15px;
  white-space: normal !important;
}
::v-deep .el-input-group__prepend {
  padding: 0 4px;
}
.block-page {
  display: flex;
  justify-content: space-between;
  margin-top: 15px;
  z-index: 101;
  position: relative;
}
.pagination {
  margin-top: 0px !important;
  display: flex;
  &.hide {
    ::v-deep {
      .el-pagination__jump,
      button,
      .el-pager {
        display: none;
      }
    }
  }
}
.extrac-parse {
  display: flex;
  ::v-deep {
    .el-form {
      display: flex !important;
      flex: 1;
      align-items: flex-start;
      .el-form-item {
        margin-bottom: 0px;
        margin-right: 15px;
        &:nth-child(2) {
          flex: 1;
        }
      }
    }
    .el-button {
      width: auto;
      height: 32px;
      width: 32px;
      display: flex;
      justify-content: center;
      align-items: center;
      border-radius: 6px;
      padding: 12px 20px;
      margin-top: 5px;
    }
    .split-expression {
      margin-top: 5px;
      .el-form {
        display: grid !important;
      }
      .el-form-item {
        margin-right: 0px;
      }
    }
  }
}
.extract-btns {
  display: flex;
  .el-button {
    margin-top: 10px;
  }
}
.extract-table {
  margin-top: 20px;
}
.block-title {
  margin-bottom: 10px;
  span {
    font-size: 16px;
    color: #4259ce;
    font-weight: 600;
  }
}
.mt5 {
  margin-top: 9px;
}
.msg-right {
  display: flex;
  flex-wrap: wrap;
  .el-button {
    flex: 1;
  }
}
::v-deep {
  .el-input-number__increase,
    .el-input-number__decrease {
      height: 14px !important;
    }
}

.msg_sec {
  ::v-deep {
    .el-input-number--small {
      width: 86px;
    }
  }
}
.my-checkbox {
  display: block;
  margin-bottom: 5px;
}
.transform-json-icon {
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  margin-top: 4px;
}
.udt {
  margin-bottom: 16px;
}
.josn-wrap {
  display: inline-flex;
  align-items: start; 
  width: 100%;
  > span {
    line-height: 30px;
    color: #909399;
    background-color: #f5f7fa;
    padding: 0 5px;
    border: 1px solid #dcdfe6;
    border-radius: 4px;
    border-right: 0;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }
}
</style>
