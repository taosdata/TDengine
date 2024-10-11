<template>
  <div class="source-ui">
    <div :class="['left-ui']">
      <el-form
        :model="sourceForm"
        ref="form"
        label-width="240px"
        label-position="left"
        size="small"
        :rules="rules"
      >
        <section class="block-wrapper">
          <el-form-item
            :label="$t('name')"
            prop="name"
          >
            <el-input
              v-model="sourceForm.name"
              id="name"
              :placeholder="$t('dataIn.palceholders.taskName')"
            ></el-input>
          </el-form-item>
          <el-form-item :label="$t('type')" prop="type" class="hidden-required">
            <el-select
              v-model="sourceForm.type"
              id="type"
              :disabled="!!editId"
              @change="handleType"
            >
              <el-option
                v-for="item in definitionsList"
                :key="item.name"
                :label="item.name"
                :value="item.id"
              ></el-option>
            </el-select>
          </el-form-item>
          <el-form-item v-if="agentShow" :label="$t('agent')" prop="agent" class="hidden-required">
            <template slot="label">
              <el-tooltip placement="top" effect="light">
                <template slot="content">
                  <div v-html="$t('dataIn.needAgentTip')"></div>
                </template>
                <div>
                  <span>{{ $t('agent') }}</span>
                  <span style="margin-left: 1px">
                    <Icon name="label_info" class="info_icon_custom"></Icon>
                  </span>
                </div>
              </el-tooltip>
            </template>
            <el-select
              id="agent"
              v-model="sourceForm.agent"
              :placeholder="$t('dataIn.palceholders.agentPlaceholder')"
              clearable
            >
              <el-option
                v-for="item in agentList"
                :key="item.name"
                :label="item.name"
                :value="item.id"
              ></el-option>
            </el-select>
            <el-tooltip
              placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
            >
              <template slot="content">
                <span v-html="$t('communityTip')"></span>
              </template>
              <el-button
                :disabled="$COMMUNITY"
                @click="createAgent"
                type="primary"
                size="small"
                plain
                class="ml15"
                icon="el-icon-plus"
                >{{ $t("dataIn.createNewAgent") }}</el-button
              >
            </el-tooltip>
            <!-- <p class="custom-placeholder mt10">
              {{ $t("dataIn.needAgentTip") }}
            </p> -->
          </el-form-item>
          <el-form-item :label="$t('stream.targetDB')" prop="targetDB">
            <el-select
              id="targetDB"
              v-model="sourceForm.targetDB"
              :placeholder="$t('dataIn.palceholders.chooseTargetDbTip')"
              @change="handleTargetDB"
            >
              <el-option
                v-for="item in dbList"
                :key="item.name"
                :value="item.name"
              ></el-option>
            </el-select>
            <el-tooltip
              placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
            >
              <template slot="content">
                <span v-html="$t('communityTip')"></span>
              </template>
              <el-button
                :disabled="$COMMUNITY"
                @click="createDb"
                type="primary"
                size="small"
                plain
                class="ml15"
                icon="el-icon-plus"
                >{{ $t("data.createDatabase") }}</el-button
              >
            </el-tooltip>
          </el-form-item>
        </section>
        <ConfigForm
          v-if="currentDefinition && currentDefinition.config && sourceForm.data"
          :config="currentDefinition.config"
          :data="sourceForm.data"
          :parser="currentDefinition.parser"
          parent="data."
          :level="1"
          ref="configform"
          :isEditable="isEditable"
        />
      </el-form>
      <section class="bottom">
        <!-- <el-button @click="cancel" type="primary" class="preview-btn" size="small">{{
          $t("preview")
        }}</el-button> -->
        <!-- <el-button
          v-if="isShowEditBtn"
          class="edit-btn"
          type="primary"
          @click="edit"
          size="small"
          >{{ $t("edit") }}</el-button
        > -->
        <template>
          <el-tooltip
            placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
          >
            <template slot="content">
              <span v-html="$t('communityTip')"></span>
            </template>
            <el-button type="primary" @click="save" size="small" :loading="loading" :disabled="$COMMUNITY">{{
              isEditable && !isCopyable ? $t("saveAndApply") : $t("submit")
            }}</el-button>
          </el-tooltip>
        </template>
         <el-button @click="cancel" class="cancel-btn" size="small">{{
          $t("cancel")
        }}</el-button>
      </section>
    </div>

    <div class="right-ui">
      <div class="doc-part">
        <DocsContent
          v-if="currentDefinition?.description"
          class="mt20"
          :content="currentDefinition.description"
        ></DocsContent>
      </div>
      <ResultTable :isEditable="isEditable"></ResultTable>
      <DatasetTable :isEditable="isEditable"/>
    </div>
    <DialogCreateDb></DialogCreateDb>
  </div>
</template>
<script>
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import {
  AddSource,
  EditSource,
  refreshTask as getDataSourceDetail
} from "@/api/explorer/datain";
import DatePicker from "@/components/date-picker";
import { Message } from "element-ui";
import { debounce, parsinginZone, decrypt } from "@/utils/index";
import DialogCreateDb from "../components/addDbDialog.vue";
import Result from "../components/result.vue";
import ResultTable from "../components/transformResultTable.vue";
import DatasetTable from "../components/datasetTablePreview.vue"
import {
  getFormConfigByDataSource,
  generateFormInitData,
  getDsnData,
  NoNeedAgentType,
} from "../utils";
import BlockHeader from "../components/blockHeader.vue";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";
import ConfigForm from "../components/configForm.vue";
import _ from 'lodash';

export default {
  name: "DbSourceUI",
  provide() {
    return {
      sourceParent: this,
      getCurrentDefinition: () => this.currentDefinition,
    };
  },
  components: {
    DatePicker,
    DialogCreateDb,
    Result,
    BlockHeader,
    DocsContent,
    ConfigForm,
    ResultTable,
    DatasetTable,
  },
  props: {
    isEditable: {
      type: Boolean,
      default: false,
    },
    editId: {
      type: [Number, String],
      default: "",
    },
    isCopyable: {
      type: Boolean,
    },
  },

  data() {
    this.mb10Type = ["opcTable", "parser", "tabs"];
    return {
      language: localStorage.getItem("local_language"),
      disable: false,
      loading: false,
      btnLoading: false,
      isShowEditBtn: false,
      dbList: [],
      sourceForm: {
        name: "",
        type: "",
        targetDB: "",
        agent: "",
        data: {},
      },

      currentDefinition: null,
      parent: "data.",
      level: "1",
      editSourceConfig: null,
      oldParams: {},
    };
  },
  created() {
    if (this.isEditable) {
      this.getDataSourceDetail();
      this.isShowEditBtn = this.isCopyable ? false : true;
    }
    this.getDBLists();
  },
  computed: {
    rules() {
      return {
        name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("required", [this.$t("name")]),
          },
        ],
        targetDB: {
          required: true,
          trigger: "change",
          message: this.$t("required", [this.$t("stream.targetDB")]),
        },
      };
    },
    agentId() {
      return this.$store.state.app.currentAgentID || "";
    },
    sourceName() {
      return this.$store.state.app.currentDSName || "";
    },
    targetDatabase() {
      return this.$store.state.app.currentDBName || "";
    },
    // resume() {
    //   return this.$store.state.app.currentResume || "";
    // }
    username() {
      return localStorage.getItem("username") || ''
    },
    decryptPwd() {
      return decrypt(localStorage.getItem("pwd")) || '';
    },
    toUrl() {
      let native_url = localStorage.getItem("native_url")
      let base_url = native_url || localStorage.getItem("base_url")
      let splitArr = base_url.split('//')
      let url = splitArr[0] + "//" + this.username + ':' + encodeURIComponent(this.decryptPwd) + '@'+ splitArr[1]
      return (
        (splitArr[0].startsWith('taos') ? '' : "taos+") +
        url +
        (this.sourceForm.targetDB ? "/" + this.sourceForm.targetDB : "")
      );
    },
    definitionsList() {
      return this.$store.state.app.definitions;
    },
    agentShow() {
      return !NoNeedAgentType.includes(this.sourceForm.type);
    },
    agentList() {
      return this.$store.state.app.agentLists;
    },
    defaultSourceConfig() {
      return this.isEditable
        ? this.editSourceConfig
        : getFormConfigByDataSource(this.definitionsList);
    },
  },
  watch: {
    "$store.state.app.createStWithoutDB": {
      deep: true,
      handler(val) {
        if (val) {
          this.$refs.form.validate((valid) => {
            if (valid) {
              return true;
            } else {
              return false;
            }
          });
        }
      },
    },
    "sourceForm.targetDB": {
      deep: true,
      handler(val) {
        this.$store.commit("app/SET_CURRENT_DBNAME", val);
      },
    },
    "$i18n.locale": {
      deep: true,
      handler(val) {
        this.language = val;
        this.$nextTick(()=>{
          this.$refs.form.clearValidate();
        })
        if (this.isEditable) {
          this.getDataSourceDetail();
        }
      },
    },
    definitionsList: {
      deep: true,
      handler(val) {
        if (!this.isEditable && !this.sourceForm.type) {
          this.$INDUSTRY 
          ? this.$set(this.sourceForm, "type", "csv")
          : this.$set(this.sourceForm, "type", "tmq");
        }
      },
      immediate: true,
    },
    defaultSourceConfig: {
      deep: true,
      handler() {
        this.getDataSource();
      }
    },
    "$store.state.app.currentDBType": {
      immediate: true,
      handler(val) {
        this.showtransformer = false;
        if (!this.isEditable) {
          this.$store.commit("app/SET_FILTER_PARSE_DATA", null);
          this.$store.commit("app/SET_EXTRACT_PARSE_DATA", null);
          this.$store.commit("app/SET_ECHO_MAP_DATA", null);
          this.$store.commit("app/SET_TRANSFORM_COL_IDENTIFIED", []);
          this.$store.commit("app/SET_TRANSFORM_PARSERDATA", null);
          this.$store.commit("app/SET_TRANSFORMER_MAPCOLUMNS", null);
          this.$store.commit("app/SET_CSV_LOCAL_COLS", []);
          this.$store.commit("app/SET_CSV_TRANSFORMER_PARSER", null);
          this.$store.commit("app/SET_CSV_PARSER", null);
          this.$store.commit("app/SET_MAPPING_JOIN", "");
          this.$store.commit("app/SET_SPLIT_EXPRESS", null);
          this.$store.commit("app/SET_TRANS_RESULT_TABLE", []);
          this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
          this.$store.commit("app/SET_TRANS_FULL_PARAMS", null);
          this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", 0);
          this.$store.commit('app/SET_RESULTTB_SHOW',false)
          this.$store.commit('app/SET_HISTORIAN_ECHODATA',null)
          this.$store.commit('app/SET_HISTORIAN_DSN','')
          this.$store.commit("app/SET_STB_DEFAULT_COLUMNS",[]);
        }
        if (val == "kafka" || val == "mqtt") {
          // this.$set(this, "constmqttCols", []);
          // this.$set(
          //   this,
          //   "constmqttCols",
          //   this.$parent.uidata[0].parser.fields
          // );
          // this.showtransformer = true;
        }
      },
    },
    "sourceForm.type": {
      handler(val) {
        this.$store.commit("app/SET_CURRENT_DBTYPE", val);
        this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
        this.$store.commit("app/SET_VALDIT_OPC_FILE_RES", { valid: true });
        this.$store.commit('app/SET_CONNECTIVITY_CHECKRESULT',{})
        this.getDataSource();
        this.$nextTick(() => {
          this.$refs.form.clearValidate();
          // if (document.querySelector(".block-title.top")) {
          //   let dom = document.querySelector(".block-title.top");
          //   let top = dom.offsetTop + dom.getBoundingClientRect().height;
          //   this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", top);
          // }
        });
      },
      immediate: true,
    },
    "$store.state.app.currentDBName": {
      deep: true,
      handler(val) {
        this.sourceForm.targetDB = val;
        this.getDBLists();
      },
    },
    "$store.state.app.currentAgentID": {
      handler(val) {
        this.sourceForm.agent = val;
      },
    },
  },
  methods: {
    async getDataSourceDetail() {
      await getDataSourceDetail(this.editId)
        .then((data) => {
          this.sourceForm.type = data.from_detail.id;
          this.sourceForm.name = data.name;
          this.sourceForm.targetDB = data?.to_expand?.subject;
          this.sourceForm.agent = data.via;
          this.editSourceConfig = getFormConfigByDataSource(
            [data.from_detail],
            data.parser
          );
          // this.oldParams.from = data.from;
          this.oldParams.labels = data.labels;
          this.oldParams.name = data.name;
          this.oldParams.to = data.to;
          if (data.via) {
            this.oldParams.via = data.via;
          }
          if (data.parser) {
            this.oldParams.parser = data.parser
          }
        })
        .finally(() => {
          this.requestIng = false;
        });
    },
    getDataSource() {
      this.currentDefinition = this.defaultSourceConfig?.[this.sourceForm.type];
      if (!this.currentDefinition) return;
      this.sourceForm.data = generateFormInitData(
        this.currentDefinition?.config
      );
      this.oldParams.data = _.cloneDeep(this.sourceForm.data);
    },

    edit() {
      this.isShowEditBtn = false;
      this.clearTargetDBWhenDelete();
    },

    save() {
      this.loading = true
      let status = this.$parent.currentTaskStatus;
      if (
        this.isEditable &&
        !this.isCopyable &&
        !["stopped", "completed"].includes(status)
      ) {
        this.$confirm(this.$t("dataIn.saveTip"), this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.submit(true);
          })
          .catch(() => {this.loading = false});
      } else {
        this.submit(true);
      }
    },
    //验证从服务器检索
    validateRetrieve(){
      this.$refs.form.validate(async (valid) => {
        if(valid){
          return true
        }else{
          return false
        }
      })
    },
    isEqualParams(obj1, obj2) {
      return _.isEqualWith(obj1, obj2, (item1, item2) => {
        if (_.isArray(item1) && _.isArray(item2)) {
          return _.isEqual(item1.sort(), item2.sort());
        }
      });
    },
    async submit() {
      this.$refs.form.validate(async (valid) => {
        if (valid) {
          if (this.sourceForm.type == "csv") {
            let flag=this.$refs.configform.$refs.csvdata[0].submitUpload()
            if(!flag){
              this.loading = false;
              return
            }
          }
          const type = this.sourceForm.type;
          let dsn = getDsnData(this.sourceForm.data, this.currentDefinition);
          dsn = type === "tmq" ? dsn : (type === 'csv' ? type + ':' + dsn : type + dsn)
           if (this.sourceForm.type.startsWith('opc') 
              && dsn.includes('csv_config_file')
              && !this.$store.state.app.validOpcFileRes?.valid
            ) {
            this.$error(this.$store.state.app.validOpcFileRes.message)
            this.loading = false;
            return
          }
          if (this.sourceForm.type !== "csv") {
            await this.$refs.configform.$refs.checkConnectivity[0].getValidateResult(dsn,this.sourceForm.agent);
            const { valid, support} = this.$store.state.app.connectivityCheckResult
            if (!valid || !support) {
              this.loading = false;
              this.$nextTick(() => {
                document.querySelector('.source-ui .left-ui .box-check-connectivity')?.scrollIntoView();
              });
              return
            }
          }
          if (this.sourceForm.type == "pibackfill") {
            const regex = /BackfillEndTime=([^&]+)/;
            const match = dsn.match(regex);
            if (match) {
              const backfillEndTimeValue = new Date(decodeURIComponent(match[1])).getTime();
              const currentTime = new Date().getTime() 
              if (backfillEndTimeValue > currentTime) {
                this.$error(this.$t('dataIn.backfillEndTimeTip'))
                this.loading = false;
                return
              }
            } 
          }
          let id = localStorage.getItem("local_clusterID");
          // this.requestIng = true;
          const params = {
            from: dsn,
            name: this.sourceForm.name,
            to: this.toUrl,
            labels: [
              "type::datain",
              `cluster-id::${id}`,
              `user::${localStorage.getItem("username")}`,
            ],
            // trigger: { "resume": this.resume }
          };
          if (this.sourceForm.agent) {
            params["via"] = this.sourceForm.agent;
          }
          if (this.sourceForm.type == "csv") {
            await this.$refs.configform.$refs.csvdata[0].$refs.transform.getTransformerParams();
            if (this.$refs.configform.$refs.csvdata[0].$refs.transform.isbreak) {
              this.loading = false;
              return
            }
            params.parser = this.$store.state.app.transformerfullparams;
          }
          if (this.sourceForm.data.parser) {
            await this.$refs.configform.$refs.transform[0].getTransformerParams();
            if (this.$refs.configform.$refs.transform[0].isbreak) {
              this.loading = false;
              return
            }
            params.parser = this.$store.state.app.transformerfullparams;
          }
          
          if (this.isEditable && this.editId && !this.isCopyable) {
            const newParams = _.cloneDeep(params)
            delete newParams.from
            newParams.data = this.sourceForm.data
            if (!this.isEqualParams(this.oldParams,newParams)) {
              let result = await EditSource(params, this.editId);
              this.loading = false;
              if (result.message) {
                this.$error(result.message);
                return;
              }
            }
            this.$parent.changeEditable(false);
            this.$parent.currentName = "dbsource";
            this.$refs.form.resetFields();
          } else {
            let result = await AddSource(params);
            this.loading = false;
            if (result.message) {
              this.$error(result.message);
              return;
            }
            this.$refs.form.resetFields();
            this.$parent.changeEditable(false);
            this.$parent.currentName = "dbsource";
          }
        } else {
          this.$nextTick(() => {
            document
              .querySelector(".source-ui .left-ui .is-error")
              ?.scrollIntoView();
          });
          this.loading = false;
          return false;
        }
      });
    },

    cancel() {
      this.$parent.currentName = "dbsource";
    },
    async getDBLists() {
      try {
        let data = await getDBListReq();
        this.dbList = data.filter((v) => v.name !== "audit" && v.name !== 'log');

        // 在编辑状态下，判断如果 targetDb 不为空，并且 targetDB 不在 dbList 中，则将 targetDB 置空
        if (this.isCopyable || this.isEditable) {
          this.clearTargetDBWhenDelete();
        }

      } catch (error) {
        console.log(error);
      }
    },

    clearTargetDBWhenDelete() {
      if (this.sourceForm.targetDB 
            && !this.dbList.find((v) => v.name === this.sourceForm.targetDB)) {
          this.sourceForm.targetDB = "";
        }
    },

    createAgent() {
      this.$store.commit("app/SET_AGENT_DIALOG", true);
      this.$store.commit("SET_DIALOG", {
        component: () => import("../components/addAgent.vue"),
        config: {
          width: "620px",
          title: this.$t("dataIn.createNewAgent"),
        },
        params: {
          showTitle: false,
          close: () => {
            this.$store.commit("SET_DIALOG_VISIBLE", false);
          },
        },
        listeners: {
          close: () => {
            this.$store.commit("SET_DIALOG_VISIBLE", false);
          },
        },
      });
    },
    createDb() {
      this.$store.commit("dbs/HANDLE_ADD_DB");
      this.$store.commit("dbs/SET_ADD_DB_COMP", "datain");
      this.$store.commit("dbs/SET_DIALOG_DB_VISABLE", true);
    },
    handleType() {
      this.sourceForm.agent = "";
    },
    handleTargetDB() {
      // 在任何状态下目标数据库改变清空超级表和 mapping table
      if (this.$store.state.app.supportTransform) {
        this.$refs.configform.$refs.transform[0].clearStbMapping()
      }
    }
  },
};
</script>

<style lang="scss" scoped>
.source-ui {
  justify-content: space-between;
  overflow-x: auto;
  display: flex;
  :deep {
    .el-input__inner {
      border: none !important;
      box-shadow: inset 0 0 0 1px rgb(190, 188, 188);
    }
  }
  .left-ui.readable {
    position: relative;
    &::before {
      content: "";
      display: block;
      background: #f2f6fc40;
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 100;
    }
  }

  .left-ui {
    flex-shrink: 0;
    width: 50%;
    min-width: 800px;
    margin-top: 10px;
    .description {
      max-width: 568px;
      overflow: auto;
    }
    section {
      border: 1px solid #ececef;
      margin-bottom: 20px;
      border-radius: 12px;
      padding: 15px;
      // border-bottom: 1px solid #ececef;
    }
    .bottom {
      display: flex;
      border: none !important;
      padding: 0px !important;
      .el-button {
        flex: 1;
      }
      .el-select {
        margin-left: 0px !important;
      }
    }
    :deep {
      .el-input-number__increase,
      .el-input-number__decrease {
        height: 30px;
        display: flex;
        justify-content: center;
        align-items: center;
      }
    }
  }
  .right-ui {
    flex: 1;
    margin-left: 40px;
    overflow: hidden;
    position: relative;
    .doc-part {
      box-shadow: rgba(0, 0, 0, 0.1) 0px 0px 15px;
      padding: 2rem;
      margin: 1rem;
      background: rgb(251, 251, 251);
      border-radius: 0.8rem;
    }
    &:deep(.markdown-body) {
      background: rgb(251, 251, 251);
      & ul,
      ol {
        padding-left: 0;
      }
    }

    // :deep {
    //   .v-note-panel {
    //     border-radius: 12px;
    //   }
    // }
  }
  .preview-btn,
  .cancel-btn,
  .edit-btn,
  .upload-flex .item {
    z-index: 101;
  }
  .custom-placeholder {
    color: $color-description;
    font-size: 14px;
    margin-top: 10px;
  }
}
</style>
