<template>
  <div class="common-transformer">
    <template>
      <section class="msg_sec">
        <div
          class="block-title"
          v-if="$store.state.app.currentDBType == 'avevaHistorian'"
        >
          <span>{{ $t("datasource.transformer.msgbody") }}</span>
        </div>
        <el-tabs
          v-model="activeName"
          v-if="$store.state.app.currentDBType !== 'csv'"
        >
          <el-tab-pane
            :disabled="
              $store.state.app.currentDBType == 'avevaHistorian' ||
              $store.state.app.currentDBType == 'csv'
            "
            :label="$t('datasource.transformer.msgbodytypes.type1')"
            name="first"
          >
            <!-- <el-radio-group
              v-model="radio"
              @change="changeCopyFormat"
              style="margin-bottom: 15px"
            >
              <el-radio v-model="radio" label="1">{{
                $t("datasource.transformer.jsonformat")
              }}</el-radio>
              <el-radio v-model="radio" label="2">{{
                $t("datasource.transformer.textformat")
              }}</el-radio></el-radio-group
            > -->
          </el-tab-pane>
          <el-tab-pane
            :disabled="$store.state.app.currentDBType !== 'avevaHistorian'"
            :label="$t('datasource.transformer.msgbodytypes.type2')"
            name="second"
          >
            <el-button type="primary" size="small" @click="getMsgBody">{{
              $t("datasource.transformer.msgbodytypes.retrieve")
            }}</el-button>
          </el-tab-pane>

          <el-tab-pane
            :label="$t('datasource.transformer.msgbodytypes.type3')"
            name="third"
          >
            <el-upload
              class="upload-demo"
              action="https://jsonplaceholder.typicode.com/posts/"
              :on-preview="handlePreview"
              :on-remove="handleRemove"
              :before-remove="beforeRemove"
              :on-change="handleChange"
              accept=".csv,.json"
              :on-exceed="handleExceed"
              :file-list="fileList"
              style="margin-bottom: 15px"
            >
              <el-button size="small" type="primary">{{
                $t("datasource.transformer.msgbodytypes.type3")
              }}</el-button>
            </el-upload>
          </el-tab-pane>
        </el-tabs>
        <!-- <keep-alive>
          <JsonEditor
            v-if="radio == '1' && activeName == 'first'"
            ref="jsoneditor"
            @change="getJsonText"
            :value="jsonvalue"
          ></JsonEditor>
        </keep-alive> -->

        <el-form
          @submit.native.prevent
          :model="msgForm"
          :rules="msgRules"
          ref="msgForm"
          >
          <!-- v-if="radio == '2' && activeName == 'first'" -->
          <el-form-item
            prop="msgbody"
            v-if="$store.state.app.currentDBType !== 'avevaHistorian'"
          >
            <el-input
              :disabled="
                $store.state.app.currentDBType == 'avevaHistorian' ||
                $store.state.app.currentDBType == 'csv'
              "
              class="msgbody"
              v-model="msgForm.msgbody"
              :placeholder="$t('datasource.transformer.msgbodytip')"
              size="small"
              type="textarea"
              :autosize="{ minRows: 5, maxRows: 5 }"
            ></el-input>
          </el-form-item>
        </el-form>
      </section>
      <section class="extract">
        <div class="block-title top">
          <span>{{
            $store.state.app.currentDBType == "csv" ||
            $store.state.app.currentDBType == "avevaHistorian"
              ? $t("datasource.transformer.identified")
              : $t("datasource.transformer.parse")
          }}</span>
        </div>
        <div
          v-if="
            $store.state.app.currentDBType !== 'avevaHistorian' &&
            $store.state.app.currentDBType !== 'csv'
          "
          class="transdescription"
          v-html="$t('datasource.transformer.extractdesc')"
        ></div>
        <div
          class="extrac-parse"
          v-if="
            $store.state.app.currentDBType !== 'csv' &&
            $store.state.app.currentDBType !== 'avevaHistorian'
          "
        >
          <el-form :rules="parseRules" :model="parseruleForm">
            <el-form-item prop="type">
              <el-select
                size="small"
                :placeholder="$t('datasource.transformer.filter_type')"
                v-model="parseruleForm.type"
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
                v-else
                v-model="parseruleForm.expression"
                :placeholder="
                  parseruleForm.type == 'json'
                    ? $t('datasource.transformer.expre_input')
                    : '(?<y>[0-9]{4})-(?<m>[0-9]{2})-(?<d>[0-9]{2})'
                "
                size="small"
              ></el-input>
              <!-- :disabled="$parent.$parent.$parent.isEditable" -->
            </el-form-item>
            <!-- <span style="color: red; font-size: 24px"
              >{{ isjson
              }}{{ activeName == "first" && radio == "1" && isjson }}</span
            > -->
            <el-button
              size="small"
              icon="el-icon-PREVIEW"
              @click="submitParse"
              style="display: flex"
              :disabled="msgForm.msgbody == ''"
            ></el-button>
              <!-- :disabled="
                (activeName == 'first' &&
                  radio == '2' &&
                  msgForm.msgbody == '') ||
                (activeName == 'first' && radio == '1' && !isjson)
              " -->
            <!-- || $parent.$parent.$parent.isEditable -->
          </el-form>
        </div>
      </section>
      <section v-if="columnsArr.length > 0">
        <!-- <div class="block-title sub">
          <span>{{ $t("datasource.transformer.identified") }}</span>
          <el-tooltip
            :content="$t('datasource.transformer.previewmore')"
            placement="bottom"
            effect="light"
          >
            <span
              class="prew"
              v-if="columnsArr.length > 0"
              @click="showIndentifyResulttb"
              >{{ $t("datasource.transformer.preview") }}</span
            >
          </el-tooltip>
        </div> -->
        <ul
          :class="[
            'col-list',
            $store.state.app.transresultname ==
            $t('datasource.transformer.identified')
              ? 'active'
              : '',
          ]"
        >
          <!-- <el-tooltip
            class="item"
            effect="light"
            :content="$t('datasource.transformer.sampleval')"
            placement="top-start"
          >
            <li :class="['col', columnsArr[0]?.name == 'ts' ? 'origin' : '']">
              <span>{{ columnsArr[0]?.name }}</span>
            </li>
          </el-tooltip> -->
          <li v-for="(item, index) in columnsArr" :key="index">
            <!-- <el-tooltip
              class="item"
              effect="light"
              :content="item.value"
              placement="top-start"
              v-if="item.name == 'ts' && index == 0"
            >
              <span>{{ item.name }}</span>
            </el-tooltip> -->
            <span>{{ item.name }}</span>
          </li>
          <li v-if="columnsArr.length > 9">
            <el-tooltip
              :content="$t('datasource.transformer.viewmore')"
              placement="top"
              effect="light"
              ><span @click="showIndentifyResulttb"
                ><i class="el-icon-more"></i
              ></span>
            </el-tooltip>
          </li>
        </ul>
      </section>
      <section class="extract">
        <div
          class="block-title sub"
          style="justify-content: flex-start; align-items: baseline"
        >
          <span>{{ $t("datasource.transformer.extract") }}</span>
          <el-tooltip placement="top" effect="light">
            <template slot="content">
              <div v-html="$t('datasource.transformer.subextractdesc')"></div>
            </template>
            <span style="margin-left: 6px"
              ><i class="el-icon-warning"></i
            ></span>
          </el-tooltip>
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
        <div class="extract-btns">
          <el-button
            type="primary"
            size="small"
            @click="addNewExtract"
            :disabled="columnsArr.length == 0"
          >
            {{ $t("datasource.transformer.addExtract") }}
          </el-button>
        </div>
      </section>
      <section class="filter">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.filter") }}</span>
        </div>
        <div
          class="transdescription"
          v-html="$t('datasource.transformer.filterdesc')"
        ></div>
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
        <el-button
          type="primary"
          size="small"
          @click="addNewFilter"
          :disabled="filterArr.length >= 1 || columnsArr.length == 0"
        >
          {{ $t("datasource.transformer.addfilter") }}
        </el-button>
      </section>
      <section style="margin-bottom: 20px">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.superconfig") }}</span>
        </div>
        <div class="table-content">
          <div class="table-title">
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
                    @change="getSTbaleList"
                    :disabled="
                      $store.state.app.currentDBName == '' ||
                      columnsArr.length == 0
                    "
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
            <el-button
              type="primary"
              size="small"
              @click="createStable"
              :disabled="$store.state.app.currentDBName == ''"
            >
              {{ $t("datasource.transformer.createstb") }}
            </el-button>
          </div>
          <div class="table-detail" v-if="tableData.length > 0">
            <el-table :data="pageTableData" border style="width: 100%">
              <template v-for="(item, index) in st_columnLists">
                <el-table-column
                  :key="index"
                  prop="Name"
                  show-overflow-tooltip
                  :label="item"
                  v-if="item == 'Name'"
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

                      <span>{{ scope.row["Name"] }}</span>
                    </div>
                  </template>
                </el-table-column>
                <el-table-column
                  v-else-if="item === 'Expression'"
                  :key="index"
                  :prop="item"
                  :show-overflow-tooltip="item === 'Expression' ? false : true"
                  :label="item"
                  width="320px"
                  :class="['normal', item.exprname == 'join' ? 'joincol' : '']"
                >
                  <template slot-scope="scope">
                    <template v-if="scope.row['Name'] == 'SubTableName'">
                      <el-form
                        ref="subtb"
                        :model="subrule"
                        :rules="subnameRule"
                        @submit.native.prevent
                      >
                        <el-form-item prop="subname">
                          <el-input
                            size="small"
                            v-model="scope.row.Expression"
                            :placeholder="exprformat"
                            @input="changeSubname"
                          ></el-input>
                        </el-form-item>
                      </el-form>
                    </template>
                    <template v-else>
                      <el-select
                        size="small"
                        v-model="scope.row.exprname"
                        style="width: 100px; margin-right: 10px"
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
                      <!-- <el-cascader
                        size="small"
                        style="width: 100px; margin-right: 10px"
                        :show-all-levels="false"
                        v-model="scope.row.maptype[1]"
                        v-if="scope.row['Type'] != 'Tablename'"
                        @change="changeMapColumn(scope)"
                        :options="options"
                      ></el-cascader> -->
                      <template
                        v-if="
                          ['mapping', 'sum', 'join'].includes(
                            scope.row.exprname
                          )
                        "
                      >
                        <el-select
                          v-model="scope.row.Expression"
                          :placeholder="$t('datasource.transformer.coltip')"
                          :clearable="scope.row.exprname == 'mapping'"
                          size="small"
                          filterable
                          :key="Math.random()"
                          @change="changeMappingExpr(scope)"
                          style="width: 180px"
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
                          v-if="scope.row.exprname == 'join'"
                          size="small"
                          :key="'exprjoin'"
                          style="width: 100px; margin-top: 6px"
                          v-model="joinwith"
                        >
                          <template slot="prepend">with</template>
                        </el-input>
                      </template>
                      <template v-else>
                        <el-input
                          slot="reference"
                          :key="'expr'"
                          :style="
                            scope.row.maptype[1].includes('join')
                              ? { width: '80px' }
                              : { width: '180px' }
                          "
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
                        <!-- <span><i class="el-icon-warning"></i></span> -->
                        <!-- <el-input
                          v-if="scope.row.exprname=='join'"
                          size="small"
                          style="width: 100px"
                          v-model="joinwith"
                        >
                          <template slot="prepend">with</template>
                        </el-input> -->
                      </template>
                    </template>
                  </template>
                </el-table-column>

                <el-table-column
                  v-else
                  :key="index"
                  :prop="item"
                  show-overflow-tooltip
                  :label="item"
                ></el-table-column>
              </template>
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
                type="primary"
                @click="caculateMappingResult"
                size="small"
                >{{ $t("datasource.transformer.calculate") }}</el-button
              >
            </div>
          </div>
        </div>
      </section>
      <el-dialog
        :title="$t('datasource.transformer.create_st')"
        :visible.sync="showCreateDIalog"
        width="40%"
        center
        destroy-on-close
        :append-to-body="true"
        @close="closeDialog"
        :close-on-click-modal="false"
      >
        <CreateSTB ref="createstb"></CreateSTB>
        <div class="buttons">
          <el-button type="primary" size="small" @click="createST">
            {{ $t("create") }}
          </el-button>
          <el-button size="small" @click="closeDialog">
            {{ $t("cancel") }}
          </el-button>
        </div>
      </el-dialog>
    </template>
  </div>
</template>
<script>
import ExtractSplit from "./extractSplit.vue";
import FilterExpression from "./filterExpression.vue";
import { getParser, getHistorianMsgbody } from "@/api/explorer/datain";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import { parsinginZone } from "@/utils";
import CreateSTB from "./createSTB.vue";
import { createStableReq } from "@/api/gateway/data/stables";
import SplitExpression from "./splitExpression.vue";
import { getDsnData } from "../utils.js";
import Papa from "papaparse";
import JsonEditor from "./jsonEditor.vue";
export default {
  name: "CommonTransformer",
  components: {
    ExtractSplit,
    FilterExpression,
    CreateSTB,
    SplitExpression,
    JsonEditor,
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
  data() {
    // var validateMsg = (rule, value, callback) => {
    //   if (!value) {
    //     return callback(new Error(this.$t("datasource.transformer.msgbodytip")));
    //   }
    //   setTimeout(() => {
    //     if (/^{|\[/.test(value)) {
          
    //       callback(new Error(this.$t("datasource.transformer.texttip")));
    //     } else {
    //       callback();
    //     }
    //   }, 100);
    // };
    return {
      radio: "1",
      isjson: false,
      jsonvalue: null,
      jsoneditorcont: null,
      istext: true,
      subrule: {
        subname: "",
      },
      activeName: "first",
      mqttDefaultCols: ["topic", "qos", "payload"],
      kafkaDefaultCols: ["topic", "partition", "offset", "key", "value"],
      parseTypes: ["regex", "json"],
      exprformat: "${c1}-${c2}:${c3}",
      exprexpression: "centigrade * 1.8 + 32",
      parseruleForm: {
        type: "json",
        expression: "",
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
      tempColumns: [],
      isbreak: false, //tranformer创建是否出错
      joinwith: "",
      isCSV: false,
      mapExpressionList: [
        "value",
        "generator",
        "join",
        "format",
        "sum",
        "expr",
      ],
      showFilterSect: false,
      enable: true, //只针对ts的expression的input
      timestampExpr: "",
      options: [],
      mappingcolumns: [],
      msgForm: {
        msgbody: "",
      },
      msgRules: {
        msgbody: [
          {
            // validator: validateMsg,
            trigger: "blur",
          },
        ],
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
      st_columnLists: ["Name", "Type", "Expression", "Output1", "Output2"],

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

      showCreateDIalog: false,
      stableLists: [],
      sruleForm: {
        s_name: "",
      },
      // srules: {
      //   s_name: [
      //     {
      //       required: true,
      //       trigger: "change",
      //       message: this.$t("datasource.transformer.st_input"),
      //     },
      //   ],
      // },
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
    subnameRule() {
      return {
        subname: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.tablenametip"),
          },
        ],
      };
    },
  },
  async mounted() {
    if (this.$store.state.app.currentDBType == "avevaHistorian") {
      this.activeName = "second";
    } else {
      this.activeName = "first";
    }
    if (this.parserColumns) {
      if (
        this.$store.state.app.currentDBType == "mqtt" ||
        this.$store.state.app.currentDBType == "kafka"
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
      // this.formatCSVExtract(this.$store.state.app.csvTransformerParser.columns);
    }
    await this.getInitStables();
    this.statisticCol();
  },
  methods: {
    getJsonText(data) {
      if (data instanceof Object) {
        this.isjson = true;
        this.$set(this, "jsoneditorcont", data);
        this.jsoneditorcont = data;
      } else {
        this.isjson = false;
      }
    },
    changeCopyFormat() {
      console.log(this.radio);
    },
    statisticCol() {
      this.configuredCount = this.tableData.filter(
        (item) => item["Expression"] != ""
      ).length;
    },
    changeMappingExpr(scope) {
      this.$set(this.mappingcolumns[scope.$index], "Expression", "");
    },
    changeCurrentMapExpr(scope) {
      this.$nextTick(() => {
        this.$set(this.pageTableData[scope.$index], "Expression", "");
        if (scope.row.exprname == "generator") {
          this.$set(this.pageTableData[scope.$index], "Expression", "now");
        }
      });
    },
    async getMsgBody() {
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
      let dsn = getDsnData(
        this.$parent.$parent.$parent.sourceForm.data,
        this.$parent.$parent.$parent.currentDefinition
      );
      let result = await getHistorianMsgbody(
        this.$store.state.app.currentDBType,
        encodeURIComponent(dsn)
      );
      this.msgForm.msgbody = JSON.stringify(result);
      await this.submitParse();
    },
    changeSubname(val) {
      this.subrule.subname = val;
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
    handleRemove(file, fileList) {
      console.log(file, fileList);
    },
    handlePreview(file) {
      console.log(file, "文件");
    },
    handleExceed(files, fileList) {
      this.$message.warning(
        `当前限制选择 3 个文件，本次选择了 ${files.length} 个文件，共选择了 ${
          files.length + fileList.length
        } 个文件`
      );
    },
    handleChange(file, fileList) {
      Papa.parse(file.raw, {
        header: false,
        complete: (result) => {
          console.log(result.data.join("\n"), "解析后的结果");
        },
      });
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
      if (Object.is(val, 0) || Object.is(val, false) || Object.is(val, true)) {
        return val.toString();
      }
      return val;
    },
    async submitParse(name) {
      try {
        if (!this.msgForm.msgbody) {
          Message.warning(this.$t("datasource.transformer.msgbodytip"));
          return;
        }
        let topparser = null;
        // let message = "";
        // if (this.radio == "1") {
        //   message = this.jsoneditorcont;
        // } else {
        //   if (!this.msgForm.msgbody) {
        //     Message.warning(this.$t("datasource.transformer.msgbodytip"));
        //     return;
        //   }
        //   message = this.msgForm.msgbody;
        // }
       
        // if (this.filterArr.length > 0) {
        // }
        // if (this.extractArr.length > 0) {
        // }

        if (this.$store.state.app.currentDBType == "avevaHistorian") {
          topparser = JSON.parse(this.msgForm.msgbody);
        } else {
          topparser = {
            parser: {
              parse: {
                [this.$store.state.app.currentDBType == "mqtt"
                  ? "payload"
                  : "value"]: {
                  [`${this.parseruleForm.type}`]:
                    this.parseruleForm.type == "regex"
                      ? this.parseruleForm.expression
                      : this.parseruleForm.type == "split"
                      ? this.$store.state.app.splitExpresList
                      : this.parseruleForm.expression
                      ? this.parseruleForm.expression
                          .split(";")
                          .toString()
                          .split(",")
                          .map((item) => item.trim())
                      : this.parseruleForm.expression,
                },
              },
            },
            input:
              this.$store.state.app.currentDBType == "csv"
                ? this.$store.state.app.csvTransformerParser.inputList
                : [].concat(this.generateInput()),
          };
        }
        // if (this.activeName == "first" && this.radio == "2" && !this.istext) {
        //   return;
        // }
        this.$store.commit("app/SET_TOP_PARSE", topparser);
        let result = await getParser(topparser);
        if (result.message) {
          Message.error(result.message);
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
        }
        let tbdata = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields
              .map((item, index) => {
                return [
                  item.name,
                  this.filterEmpty(data[index]) ? data[index].toString() : null,
                ];
              })
              .filter((f) => !hiddenCols.includes(f[0]))
          );
        });
        this.$store.commit("app/SET_TRANS_RESULT_TABLE", tbdata);
        if (this.filterArr.length > 0) {
          await this.$refs.filter[0].submitFilter();
        }
        if (this.extractArr.length > 0) {
          await this.$refs.extract[0].submitExtract(true);
        }
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
                  this.$store.state.app.currentDBType == "avevaHistorian"
                ) {
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
            type: "string",
            value:
              this.$t("datasource.transformer.sampleval") +
              ":" +
              (finalVal.join("") ? finalVal.join(" ; ") : ""),
          };
        });
        if (!this.$store.state.app.transresultname) {
          this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
        }
        this.showIndentifyResulttb();
      } catch (error) {
        console.log(error);
      }
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
    handleCurrentChange(val) {
      this.currentPage = val;
      this.pageTableData.splice(0, Infinity);
      this.setPageTableData();
    },

    //编辑回显数据--编辑状态不自动显示result table
    async echoParser(value) {
      if (this.$store.state.app.currentDBType == "avevaHistorian") {
        let dsn = this.$store.state.app.historiandsn;
        let result = await getHistorianMsgbody(
          this.$store.state.app.currentDBType,
          encodeURIComponent(dsn)
        );
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
        this.parseruleForm.type = Object.keys(
          value.parser.parse[tagKey]
        ).toString();
        this.parseruleForm.expression =
          this.parseruleForm.type == "regex"
            ? Object.values(value.parser.parse[tagKey]).toString()
            : Object.values(value.parser.parse[tagKey])
                .toString()
                .replace(",", ";");
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
            if (expreKey == "join") {
              this.joinwith = val[1]["with"];
            }
            return {
              columnname: val[0],
              type: expreKey,
              expression: val[1][expreKey],
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
          await Promise.all(newarr).then((val) => {
            console.log(val, "获取映射字段");
          });
        }
        if (isincludeFilter) {
          await this.$refs.filter[0].submitFilter();
        }
        this.sruleForm.s_name = value.parser.model.using;
        this.subrule.subname = value.parser.model.name;
        await this.getSTbaleList();
        await this.echoFetchMap();
        this.$store.commit("app/SET_RESULTTB_SHOW", false);
        this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
      });
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
      if (!this.validateSubName()) {
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
      if (this.tableData && !this.tableData[0]?.["Expression"]) {
        this.isbreak = true;
        return false;
      }
      this.isbreak = false;
      let tags = [];
      let columns = [];
      let mutates = [];
      let mutateMap = {};
      let primarykey = "";
      this.tableData.forEach((item) => {
        if (item["Expression"]) {
          if (
            this.params_columns.includes(item["Name"]) &&
            !item["Type"].includes("TIMESTAMP")
          ) {
            columns.push(item["Name"]);
          }
          if (item["Type"].includes("TIMESTAMP") && !primarykey) {
            primarykey = item["Name"];
          }
          if (this.params_tags.includes(item["Name"])) {
            tags.push(item["Name"]);
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
              expreitem["with"] = this.joinwith;
            }
            mutates.push({
              [`${item["Name"]}`]: expreitem,
            });
          }
        }
      });

      mutates.forEach((item) => {
        Object.assign(mutateMap, item);
      });
      columns.unshift(primarykey);
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
                  .concat({
                    filter: Object.values(
                      this.$store.state.app.transformerFilterParseData
                    ).toString(),
                  })
                  .concat(this.$store.state.app.transformExtractParseData)
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
          : this.$store.state.app.currentDBType == "avevaHistorian"
          ? this.$store.state.app.topParse.input
          : [].concat(this.generateInput()),
        format: {
          pageCount: this.pageCount,
          pageSize: this.pageSize,
          currentPage: this.currentPage,
        },
      };
      // if (this.activeName == "first" && this.radio == "2" && !this.istext) {
      //   return;
      // }
      if (tags.length == 0 || columns.length == 0 || !primarykey) {
        Message.warning(this.$t("datasource.transformer.mappingvaildtip"));
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
    changeMapColumn(scope) {
      if (scope.row.maptype[1][0] == "mapping") {
        this.enable = false;
        this.$set(
          this.tableData[scope.$index],
          "Expression",
          scope.row.maptype[1][1]
        );
      } else {
        this.enable = true;
      }
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
            item.type == "regex"
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
          : this.$store.state.app.currentDBType == "avevaHistorian"
          ? this.$store.state.app.topParse.input
          : [].concat(this.generateInput()),
        format: {
          pageCount: this.pageCount,
          pageSize: this.pageSize,
          currentPage: this.currentPage,
        },
      };
      if (this.activeName == "first" && this.radio == "2" && !this.istext) {
        return;
      }
      this.$store.commit("app/SET_TRANS_FULL_PARAMS", parserData);
      // this.$emit("getTransformerParams", parserData);
    },
    changeColumnStatus(index, name) {
      //选中的列不能再选中
      let ind = this.columnsArr.findIndex((item) => item.name == name);
      this.$set(this.columnsArr[ind], "show", false);
      this.extractAddStatus = this.columnsArr.every((item) => !item.show);
      this.$set(this.extractArr[index], "columnname", name);
    },
    async getParserData(data) {
      try {
        let result = await getParser(data);
        if (result.message) {
          Message.error(result.message);
          this.isbreak = true;
          return;
        }
        this.isbreak = false;
        let outputColumns = result[0].fields.map((item) => item.name);
        let outputTBData = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, data[index]];
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
        this.tableData.map((item) => {
          item[`Output1`] = "";
          item[`Output2`] = "";
          if (overlapColumns.includes(item["Name"])) {
            outputTBData.map((val, index) => {
              item[`Output` + (index + 1)] =
                item["Name"] == "SubTableName"
                  ? val["__tbname__"]
                  : this.filterEmpty(val[item["Name"]])
                  ? val[item["Name"]].toString()
                  : "";
            });
          }
          return item;
        });
        this.setPageTableData();
      } catch (error) {
        console.log(error);
      }
    },
    //输出input结果
    generateInput() {
      let inputList = [];
      let resultMsgbody = "";
      // if (this.radio == "1") {
      //   resultMsgbody = Array.isArray(this.jsoneditorcont)
      //     ? this.jsoneditorcont.map((item) => JSON.stringify(item))
      //     : [].concat(JSON.stringify(this.jsoneditorcont));
      // } else {
      //   if (/^{|\[/.test(this.msgForm.msgbody)) {
      //     // Message.error(this.$t("datasource.transformer.texttip"));
      //     this.istext = false;
      //     return;
      //   }
      //   this.istext = true;
      //   resultMsgbody = this.msgForm.msgbody
      //     .replace(/[\n\s]/g, "*&$*")
      //     .split("*&$*");
      // }
      if (this.msgForm.msgbody.replace(/\}\s*\{/g, "}{").includes("}{")) {
        resultMsgbody = this.msgForm.msgbody
          .replace(/\}\s*\{/g, "}&${")
          .split("&$");
      } else {
        if (
          /\n/g.test(this.msgForm.msgbody) &&
          /^[^\{]/.test(this.msgForm.msgbody.trim())
        ) {
          //普通文本，目前第一列暂时不能为json格式
          resultMsgbody = this.msgForm.msgbody
            .replace(/[\n\s]/g, "*&$*")
            .split("*&$*");
        } else {
          try {
            if (
              /^\{/g.test(this.msgForm.msgbody) &&
              JSON.parse(this.msgForm.msgbody)
            ) {
              resultMsgbody = [].concat(this.msgForm.msgbody);
            }
          } catch (error) {
            Message.error(this.$t("datasource.transformer.jsontip"));
            return;
          }

          resultMsgbody = this.msgForm.msgbody.split(";");
        }
      }
      inputList = resultMsgbody.map((msg) => {
        let inputobj = {};
        this.indentifiedColumns.forEach((item) => {
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
          }
        });
        return inputobj;
      });
      return inputList;
    },
    submitSuper(data) {
      if (!this.msgForm.msgbody) {
        Message.error(this.$t("datasource.transformer.msgbodytip"));
        return;
      }
      if (!this.tableData[0]["Expression"]) {
        Message.error(this.$t("datasource.transformer.tablenametip"));
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
      // if (this.activeName == "first" && this.radio == "2" && !this.istext) {
      //   return;
      // }
      this.getParserData(parserData);
    },
    closeDialog() {
      this.dialogForm.st_name = "";
      this.showCreateDIalog = false;
    },
    //创建或者查询
    async createST() {
      this.$refs.createstb.$refs.form.validate(async (valid) => {
        if (!valid) return false;
        if (valid) {
          try {
            const { ts_field_name, tags, columns } =
              this.$refs.createstb.stable_form;
            if (!ts_field_name) {
              return Message.warning(
                this.$t("dataIn.enterTip") + " " + this.$t("data.columnNameTip")
              );
            }
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
            let payload = {
              selected_db: this.$store.state.app.currentDBName,
              stable_form: this.$refs.createstb.stable_form,
            };
            let result = await createStableReq(payload);
            if (result?.desc) {
              Message.error(this.$t(result.desc));
              return;
            }
            Message.success(this.$t("operateSucc"));
            await this.getInitStables();
            this.sruleForm.s_name = this.$refs.createstb.stable_form.name;
            this.getSTbaleList();
            this.closeDialog();
          } catch (error) {
            error.desc ? Message.error(error.desc) : "";
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

      this.showCreateDIalog = true;
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
          }
          return item;
        });
        this.$set(
          this.tableData[0],
          "Expression",
          echoData.model.name.toString()
        );
        this.caculateMappingResult();
      }
    },
    async getSTbaleList() {
      try {
        this.currentPage = 1;
        let res = await sendSQLReq(
          `desc \`${this.$store.state.app.currentDBName}\`.\`${this.sruleForm.s_name}\``
        );
        let precision = await sendSQLReq(`
        select \`precision\` from information_schema.ins_databases where name = '${this.$store.state.app.currentDBName}'
        `);
        if (res.desc) {
          Message.error(res.desc);
          return;
        }
        if (this.extractArr.length > 0) {
          // await this.getAllExtract(true);
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
                }
                if (
                  this.$store.state.app.currentDBType == "kafka" &&
                  !this.kafkaDefaultCols.includes(val.value)
                ) {
                  return val;
                } else if (
                  this.$store.state.app.currentDBType == "avevaHistorian"
                ) {
                  return val;
                }
              })
          );
        }
        let defaultmap = this.options
          .filter((item) => item.value == "mapping")[0]
          .children.map((label) => label.label);
        this.params_columns.splice(0, this.params_columns.length - 1);
        this.params_tags.splice(0, this.params_tags.length - 1);
        this.pageCount = res.data.length + 1;
        this.tableData = res.data.map((val, index) => {
          if (!val[3] && index > 0) {
            this.params_columns.push(val[0]); //存储非主键列
          }
          if (val.includes("TAG")) {
            this.params_tags.push(val[0]);
          }
          let equalindex = defaultmap.findIndex(
            (item) => item.toLowerCase() == val[0].toLowerCase()
          );
          return {
            Name: val[0],
            Type:
              val[1] == "TIMESTAMP"
                ? val[1] + "(" + precision.data[0][0] + ")"
                : val[1],
            exprname: "mapping",
            maptype:
              equalindex > -1
                ? ["mapping", `${defaultmap[equalindex]}`]
                : ["expression", "value"],
            Expression: equalindex > -1 ? defaultmap[equalindex] : "",
            Output1: "",
            Output2: "",
          };
        });

        this.tableData.unshift({
          Name: "SubTableName", //this.sruleForm.s_name,
          Type: "Tablename",
          exprname: "mapping",
          maptype: ["expression", "string"],
          Expression: "",
          Output1: "",
          Output2: "",
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
        this.submitParse();
      });
    },
    deleteExtract(index, name) {
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
          this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
        }
        let oldextract = this.$store.state.app.transformExtractParseData;

        if (oldextract && Object.keys(oldextract.extract).includes(name)) {
          delete oldextract.extract[name];
        }

        if (name) {
          let ind = this.extractArr.findIndex(
            (item) => item.columnname == name
          );
          this.extractArr.splice(ind, 1);
          let restoreIndex = this.columnsArr.findIndex(
            (item) => item.name == name
          );
          this.$set(this.columnsArr[restoreIndex], "show", true);
        } else {
          this.extractArr.splice(index, 1);
        }
        if (this.extractArr.length > 0) {
          if (this.filterArr.lenght > 0 && this.$refs.filter[0].isexecuted) {
            this.$refs.filter[0].submit();
          } else {
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
    joinwith: {
      deep: true,
      handler(val) {
        this.$store.commit("app/SET_MAPPING_JOIN", val);
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
    "$store.state.app.transformerParserData": {
      deep: true,
      handler(val) {
        console.log(val, "监听transform数据回显");
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
          this.$store.state.app.currentDBType == "kafka"
        ) {
          this.initColumnLists(val.filter((item) => item.name != "ts"));
        } else {
          this.initColumnLists(val);
        }
      },
    },
    "$store.state.app.currentDBType": {
      deep: true,
      handler(val) {
        if (val == "avevaHistorian") {
          this.activeName = "second";
        } else {
          this.activeName = "first";
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
.extract {
  .el-button {
    width: 100%;
    margin-top: 15px;
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
  // &.active {
  //   padding: 20px;
  //   animation: heart 5s linear infinite;
  // }
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
.filter {
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
  ::v-deep {
    .el-table {
      .cell {
        // display: flex;
      }
      thead tr th {
        background-color: #f5f7fa;
      }
      .el-table__cell {
        padding: 6px 0 !important;
      }
      tbody tr:first-child {
        .cell {
          // padding-bottom: 16px !important;
        }
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
</style>
