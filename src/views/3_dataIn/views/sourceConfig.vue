<template>
  <div class="source-ui">
    <div :class="['left-ui', isShowEditBtn ? 'readable' : '']">
      <!-- <section>
        <DataTarget ref="sourceTop"></DataTarget>
      </section> -->
      <el-form
        :model="sourceForm"
        ref="form"
        label-width="200px"
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
              :placeholder="$t('dataIn.palceholders.taskName')"
            ></el-input>
          </el-form-item>
          <el-form-item
            :label="$t('type')"
            prop="type"
          >
            <el-select
              v-model="sourceForm.type"
              placeholder=""
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
            <!-- <el-button
            class="ml10"
            type="primary"
            plain
            >{{ $t('plan.price') }}</el-button
          > -->
          </el-form-item>
          <el-form-item
            v-if="agentShow"
            :label="$t('agent')"
            prop="agent"
          >
            <el-select
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
            <el-button
              @click="createAgent"
              type="primary"
              size="small"
              class="ml"
              icon="el-icon-plus"
              >{{ $t('dataIn.createNewAgent') }}</el-button
            >
            <p class="custom-placeholder mt10">{{ $t('dataIn.needAgentTip') }}</p>
          </el-form-item>
          <el-form-item
            :label="$t('stream.targetDB')"
            prop="targetDB"
          >
            <el-select
              v-model="sourceForm.targetDB"
              :placeholder="$t('dataIn.palceholders.chooseTargetDbTip')"
            >
              <el-option
                v-for="item in dbList"
                :key="item.name"
                :value="item.name"
              ></el-option>
            </el-select>
            <el-button
              @click="createDb"
              type="primary"
              size="small"
              class="ml"
              icon="el-icon-plus"
              >{{ $t('data.createDatabase') }}</el-button
            >
          </el-form-item>
        </section>
        <ConfigForm
          v-if="currentDefinition && currentDefinition.config"
          :config="currentDefinition.config"
          :data="sourceForm.data"
          parent="data."
          :level="1"
        />
      </el-form>
      <section class="bottom">
        <el-button
          v-if="isShowEditBtn"
          class="edit-btn"
          type="primary"
          @click="edit"
          size="small"
          >{{ $t("edit") }}</el-button
        >
        <el-button v-else type="primary" @click="save" size="small">{{
          isEditable && !isCopyable ? $t("save") : $t("add")
        }}</el-button>
        <el-button @click="cancel" class="cancel-btn" size="small">{{
          $t("cancel")
        }}</el-button>
      </section>
    </div>

    <div class="right-ui">
      <!-- <mavon-editor
        v-model="dbsource[0].description"
        :toolbarsFlag="false"
        :default-open="'preview'"
        :subfield="false"
      /> -->
      <div class="doc-part">
        <DocsContent
          v-if="currentDefinition?.description"
          class="mt20"
          :content="currentDefinition.description"
        ></DocsContent>
      </div>
    </div>
    <DialogCreateDb></DialogCreateDb>
  </div>
</template>
<script>
import DataTarget from "./dataTarget.vue";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import {
  AddSource,
  EditSource,
  validateTask,
  refreshTask as getDataSourceDetail
} from "@/api/explorer/datain";
import DatePicker from "@/components/date-picker";
import { Message } from "element-ui";
import { debounce, parsinginZone, decrypt } from "@/utils/index";
import DialogCreateDb from "../components/addDbDialog.vue";
import Result from "../components/result.vue";
import {
  getFormConfigByDataSource,
  generateFormInitData,
  getDsnData,
  NoNeedAgentType
} from "../utils";
import FormItem from "../components/formItem.vue";
import BlockHeader from "../components/blockHeader.vue";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";
import ConfigForm from "../components/configForm.vue";

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
    DataTarget,
    Result,
    FormItem,
    BlockHeader,
    DocsContent,
    ConfigForm,
  },
  props: {
    dbsource: {
      type: Array,
      default() {
        return [];
      },
    },
    tagName: {
      type: String,
      default: "datasource",
    },
    isEditable: {
      type: Boolean,
      default: false,
    },
    editId: {
      type: [Number, String],
      default: 0,
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
        name: '',
        type: '',
        targetDB: '',
        agent: '',
        data: {},
      },
      rules: {
        name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t('required', [this.$t("name")]),
          },
        ],
        targetDB: {
          required: true,
          trigger: "change",
          message: this.$t('required', [this.$t("stream.targetDB")]),
        },
      },
      currentDefinition: null,
      parent: "data.",
      level: "1",
      editSourceConfig: null,
    };
  },
  created() {
    if (this.isEditable) {
      this.getDataSourceDetail()
      this.isShowEditBtn = this.isCopyable ? false : true;
    } 
    this.getDBLists()
  },
  mounted() {
    if (!this.editId) {
      this.sourceForm.type = 'tmq';
    }
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
    toUrl() {
      return 'taos+' + localStorage.getItem("base_url") + (this.sourceForm.targetDB ? "/" + this.sourceForm.targetDB : "");
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
      return this.isEditable ? this.editSourceConfig : getFormConfigByDataSource(this.definitionsList);
    },
  },
  watch: {
    "$i18n.locale": {
      deep: true,
      handler(val) {
        this.language = val;
      },
    },
    dbsource: {
      deep: true,
      handler(val) {
        this.$forceUpdate();
        this.getDataSource();
      },
      immediate: true
    },
    tagName: {
      deep: true,
      handler(val) {
        this.$forceUpdate();
      },
    },
    'sourceForm.type': {
      handler() {
        this.getDataSource();
      },
      immediate: true
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
        this.sourceForm.agent = val
      }
    }
  },
  methods: {
    async getDataSourceDetail() {
      await getDataSourceDetail(this.editId)
        .then(data => {
          this.sourceForm.type = data.from_detail.id;
          this.sourceForm.name = data.name;
          this.sourceForm.targetDB = data?.to_expand?.subject;
          this.sourceForm.agent = data.via;
          this.editSourceConfig = getFormConfigByDataSource([data.from_detail], data.parser);
        })
        .finally(() => {
          this.requestIng = false;
        });
    },
    getDataSource() {
      this.currentDefinition = this.defaultSourceConfig?.[this.sourceForm.type];
      console.log("currentDefinition", this.currentDefinition);
      if (!this.currentDefinition) return;
      this.sourceForm.data = generateFormInitData(
        this.currentDefinition?.config
      );
    },

    edit() {
      this.isShowEditBtn = false;
    },

    save() {
      let status = this.$parent.currentTaskStatus
      if (this.isEditable && !this.isCopyable && !['stopped','completed'].includes(status)) {
        this.$confirm(this.$t("dataIn.saveTip"), this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.submit(true);
          })
          .catch(() => {});
      } else {
        this.submit(true);
      }
    },

    submit() {
      this.$refs.form.validate(async (valid) => {
        if (valid) {
          const dsn = getDsnData(this.sourceForm.data, this.currentDefinition,);
          const type = this.sourceForm.type;
          let id = localStorage.getItem("local_clusterID");
          // this.requestIng = true;
          const params = {
            from: type === "tmq" ? dsn : type + dsn,
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
          if (this.sourceForm.data.parser) {
            params.parser = this.sourceForm.data.parser;
          }
          if (this.isEditable && this.editId && !this.isCopyable) {
            let result = await EditSource(params, this.editId);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.changeEditable(false);
            this.$parent.toggleComponent("tmqtable");
            this.$refs.form.resetFields();
          } else {
            let result = await AddSource(params);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.changeEditable(false);
            this.$parent.toggleComponent("tmqtable");
            this.$refs.form.resetFields();
          }
        } else {
          this.$nextTick(() => {
            document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
          });
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
        this.dbList = data.filter(v => v.name !== 'audit')
      } catch (error) {
        console.log(error);
      }
    },
    createAgent() {
      this.$store.commit("app/SET_AGENT_DIALOG", true);
      this.$store.commit('SET_DIALOG', {
        component: () => import('../components/addAgent.vue'),
        config: {
          width: '620px',
          title: this.$t('dataIn.createNewAgent')
        },
        params: {
          showTitle: false,
          close: () => {
            this.$store.commit('SET_DIALOG_VISIBLE', false);
          }
        },
        listeners: {
          close: () => {
            this.$store.commit('SET_DIALOG_VISIBLE', false);
          }
        }
      });
    },
    createDb() {
      this.$store.commit("dbs/HANDLE_ADD_DB");
      this.$store.commit("dbs/SET_ADD_DB_COMP", "datain");
      this.$store.commit("dbs/SET_DIALOG_DB_VISABLE", true);
    },
    handleType() {
      this.sourceForm.agent = ''
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
    overflow: hidden;    .doc-part {
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
