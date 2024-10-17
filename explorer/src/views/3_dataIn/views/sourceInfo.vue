<template>
  <div class="source-ui">
    <div :class="['left-ui']">
      <section class="block-wrapper">
        <div class="mb10">
          <BlockHeader :title="$t('dataIn.basicsTitle')"> </BlockHeader>
        </div>
        <div class="descriptions">
          <div class="descItem">
            <span class="itemTitle">{{$t('name')}}:</span>
            <span>{{ sourceForm.name }}</span>
          </div>
          <div class="descItem">
            <span class="itemTitle">{{ $t('type')}}: </span>
            <span>{{ sourceForm.type }}</span> 
          </div>
          <div class="descItem">
            <span class="itemTitle">{{$t('agent')}}:</span>
            <span>{{ sourceForm.agent }}</span>
          </div>
          <div class="descItem">
            <span class="itemTitle">{{ $t('stream.targetDB') }}:</span>
            <span>{{ sourceForm.targetDB }}</span>
          </div>
        </div>
      </section>
      <ConfigDesc
        v-if="currentDefinition && currentDefinition.config && sourceForm.data"
        :config="currentDefinition.config"
        :data="sourceForm.data"
        :parser="currentDefinition.parser"
        parent="data."
        :level="1"
        ref="configform"
        :isViewable="isViewable"
      />
      <section class="bottom">
         <el-button @click="cancel" class="cancel-btn" size="small">{{
          $t("back")
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
      <ResultTable :isViewable="isViewable"></ResultTable>
      <DatasetTable :isViewable="isViewable"/>
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
import ConfigDesc from "../components/configDesc.vue";

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
    ConfigDesc,
    ResultTable,
    DatasetTable,
  },
  props: {
    isViewable: {
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
    };
  },
  created() {
    // if (this.isViewable) {
      this.getDataSourceDetail();
      this.isShowEditBtn = this.isCopyable ? false : true;
    // }
    this.getDBLists();
  },
  computed: {
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
    definitionsList() {
      return this.$store.state.app.definitions;
    },
    agentList() {
      return this.$store.state.app.agentLists;
    },
    defaultSourceConfig() {
      return this.isViewable
        ? this.editSourceConfig
        : getFormConfigByDataSource(this.definitionsList);
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
        if (this.isViewable) {
          this.getDataSourceDetail();
        }
      },
    },
    definitionsList: {
      deep: true,
      handler(val) {
        if (!this.isViewable && !this.sourceForm.type) {
          this.$set(this.sourceForm, "type", "tmq");
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
        if (!this.isViewable) {
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
        }
      },
    },
    "sourceForm.type": {
      handler(val) {
        this.$store.commit("app/SET_CURRENT_DBTYPE", val);
        this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
        this.$store.commit("app/SET_VALDIT_OPC_FILE_RES", { valid: true });
        this.getDataSource();
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

    cancel() {
      this.$parent.currentName = "dbsource";
    },
    async getDBLists() {
      try {
        let data = await getDBListReq();
        this.dbList = data.filter((v) => v.name !== "audit" && v.name !== 'log');

        // 在编辑状态下，判断如果 targetDb 不为空，并且 targetDB 不在 dbList 中，则将 targetDB 置空
        if (this.isCopyable) {
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

    handleType() {
      this.sourceForm.agent = "";
    },
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
      // border: 1px solid #ececef;
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
    .descriptions {
      font-size: 16px;
      display: grid;
      grid-template-columns: 1fr 1fr;
    }
    .block-wrapper {
      margin-bottom: 0px;
    }
    .descItem {
      padding: 0 5px 10px 0;
      .itemTitle {
        padding-right: 10px;
      }
    }

    .mb10 {
      margin-bottom: 10px;
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
