<template>
  <div class="source-ui">
    <div :class="['left-ui', isShowEditBtn ? 'readable' : '']">
      <section>
        <DataTarget ref="sourceTop"></DataTarget>
      </section>
      <el-form
        :model="sourceForm"
        ref="form"
        label-width="200px"
        label-position="left"
        size="small"
        :rules="rules"
      >
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
      <mavon-editor
        v-model="dbsource[0].description"
        :toolbarsFlag="false"
        :default-open="'preview'"
        :subfield="false"
      />
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
      type: Number,
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
      sourceForm: {
        data: {},
      },
      rules: {},
      currentDefinition: null,
      parent: "data.",
      level: "1",
      editSourceConfig: null
    };
  },
  created() {
    if (this.isEditable) {
      this.getDataSourceDetail()
      this.isShowEditBtn = this.isCopyable ? false : true;
    } 
    this.getDataSource();
  },
  mounted() {},
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
    defaultSourceConfig() {
      return this.isEditable ? this.editSourceConfig : getFormConfigByDataSource(this.dbsource);
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
    editSourceConfig: {
      handler(val) {
        if (val) {
          this.getDataSource();
        }
      },
      immediate: true
    },
  },
  methods: {
    async getDataSourceDetail() {
      await getDataSourceDetail(this.editId)
        .then(data => {
          this.editSourceConfig = getFormConfigByDataSource([data.from_detail], data.parser);
        })
        .finally(() => {
          this.requestIng = false;
        });
    },
    getDataSource() {
      this.currentDefinition = this.defaultSourceConfig?.historian;
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
      if (this.isEditable && !this.isCopyable) {
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

    validateForm() {
      this.$refs.form.validate(async (valid) => {
        if (valid) {
          const dsn = getDsnData(
            this.sourceForm.data,
            this.currentDefinition,
          );
          const type = this.tagName;
          let id = localStorage.getItem("local_clusterID");
          // this.requestIng = true;
          const params = {
            from: type === "tmq" ? dsn : type + dsn,
            name: this.sourceName,
            to:
              "taos+" +
              localStorage.getItem("base_url") +
              (this.targetDatabase ? "/" + this.targetDatabase : ""),
            labels: [
              "type::datain",
              `cluster-id::${id}`,
              `user::${localStorage.getItem("username")}`,
            ],
            // trigger: { "resume": this.resume }
          };
          if (this.agentId) {
            params["via"] = this.agentId;
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
          } else {
            let result = await AddSource(params);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.changeEditable(false);
            this.$parent.toggleComponent("tmqtable");
          }
        } else {
          console.log("error submit!!");
          return false;
        }
      });
    },
    
    async submit() {
      let sourceTop = this.$refs.sourceTop;
      sourceTop.$refs.ruleForm.validate(async valid => {
        if (valid) {
          this.validateForm()
        } else {
          return false
        }
      });
    },
    cancel() {
      this.$parent.currentName = "dbsource";
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
    width: 800px;
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
    :deep {
      .v-note-panel {
        border-radius: 12px;
      }
    }
  }
  .cancel-btn,
  .edit-btn,
  .upload-flex .item {
    z-index: 101;
  }
}
</style>
