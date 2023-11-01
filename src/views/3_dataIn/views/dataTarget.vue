<template>
  <div class="data-target">
    <el-form
      :model="ruleForm"
      ref="ruleForm"
      label-width="200px"
      :rules="rules"
      class="reqired-change"
    >
      <el-form-item :label="$t('name')" prop="name">
        <el-input
          size="small"
          v-model="ruleForm.name"
          :placeholder="$t('dataIn.palceholders.taskName')"
          @change="changeDBType"
        ></el-input>
      </el-form-item>
      <el-form-item :label="$t('type')" prop="type" size="small">
        <el-select
          v-model="ruleForm.type"
          @change="changeDBType"
          :disabled="currentEditid ? true : false"
        >
          <el-option
            v-for="item in dbTypes"
            :label="item.name"
            :key="item.id"
            :value="item.id"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item
        :label="$t('agent')"
        prop="'agent'"
        size="small"
        v-if="showAgentSelect"
      >
        <div class="transverse">
          <el-select
            v-model="ruleForm.agent"
            :placeholder="$t('dataIn.palceholders.agentPlaceholder')"
            @change="changeAgent"
          >
            <el-option
              v-for="item in agentList"
              :label="item.name"
              :key="item.id"
              :value="item.id"
            ></el-option>
          </el-select>
          <el-button
            class="ml"
            type="primary"
            @click="createAgent"
            size="small"
            icon="el-icon-plus"
            >{{ $t("taosagents.createnewagent") }}</el-button
          >
        </div>

        <p class="custom-placeholder mt10">{{ $t("dataIn.needAgentTip") }}</p>
      </el-form-item>
      <el-form-item :label="$t('stream.targetDB')" prop="dbName">
        <div class="transverse">
          <el-select
          v-model="ruleForm.dbName"
          size="small"
          :placeholder="$t('dataIn.palceholders.chooseTargetDbTip')"
          @change="changeDB"
        >
          <el-option
            v-for="db in dbList"
            :key="db['node-key']"
            :label="db.name"
            :value="db.name"
          ></el-option>
        </el-select>
        <el-button
          class="ml"
          type="primary"
          @click="handleDbBtn"
          size="small"
          icon="el-icon-plus"
          >{{ $t("data.createDatabase") }}</el-button
        >
        </div>
        
        <!-- <el-button @click="downloadFile">文件下载</el-button> -->
      </el-form-item>
      <el-form-item
        :label="$t('dataIn.resume')"
        prop="'resume'"
        size="small"
      >
        <div class="transverse">
          <el-select
            v-model="ruleForm.resume"
            :placeholder="$t('dataIn.palceholders.resumePlaceholder')"
            @change="changeResume"
          >
            <el-option
              v-for="item in resumeList"
              :label="item.name"
              :key="item.id"
              :value="item.id"
            ></el-option>
          </el-select>
        </div>

        <div class="custom-placeholder mt10" v-html="transforHtml()"></div>
      </el-form-item>
    </el-form>
    <el-dialog
      :title="$t('dataIn.createNewAgent')"
      width="620px"
      :visible.sync="showAgent"
      :destroy-on-close="true"
      @close="closeDialog"
    >
      <AddAgent :key="showAgent" :agent="{}"></AddAgent>
    </el-dialog>
  </div>
</template>
<script>
import {
  getUIData,
  getFileStream,
  downlaodAllNodes,
} from "@/api/explorer/datain";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { getAgentsData, addNewAgent } from "@/api/explorer/agent";
import marked from "marked";

import AddAgent from "../components/addAgent.vue";
export default {
  name: "DataTarget",
  components: { AddAgent },
  data() {
    return {
      currentEditid: "",
      dbTypes: [],
      dbList: [],
      agentList: [],
      showAgentSelect: false,
      showAgent: false,
      showAgentdoc: false,
      agentTypes: [
        "pi",
        "pibackfill",
        "opcua",
        "opcda",
        "influxdb",
        "opentsdb",
        "mqtt",
      ],
      ruleForm: {
        name: "",
        type: "",
        agent: "",
        dbName: "",
        resume: "",
      },
      agentrule: [
        {
          required: true,
          trigger: "change",
          message: this.$t("datasource.agenttip"),
        },
      ],
      rules: {
        name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.targetnametip"),
          },
        ],
        dbName: {
          required: true,
          trigger: "change",
          message: this.$t("datasource.selecttargetdb"),
        },
      },
      ruleAgentForm: {
        name: "",
      },
      rulesAgent: {
        name: [
          {
            message: this.$t("taosagents.rules.name"),
            trigger: "blur",
            required: true,
          },
        ],
      },
      resumeList: [
        {
          name: 'always',
          id: 'always'
        },
        {
          name: 'never',
          id: 'never'
        },
        {
          name: 'once',
          id: 'once'
        },
      ]
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleAgentForm.name) {
        return true;
      }
      return false;
    },
  },
  mounted() {
    this.getDbTypes();
    this.getDBLists();
    this.getAgents();
    this.getInitValue();
    this.currentEditid = this.$store.state.app.currentEditID;
  },
  methods: {
    closeDialog() {
      this.$store.commit("app/SET_AGENT_DIALOG", false);
    },
    setTaskName() {
      this.$store.commit("app/SET_CURRENT_DSNAME", this.ruleForm.name);
    },
    changeAgent() {
      this.$store.commit("app/SET_CURRENT_AGENT", this.ruleForm.agent);
    },
    changeDB() {
      this.$store.commit("app/SET_CURRENT_DBNAME", this.ruleForm.dbName);
    },
    changeResume() {
      this.$store.commit("app/SET_CURRENT_RESUME", this.ruleForm.resume);
    },
    async getAllPoints() {
      try {
        let result = await downlaodAllNodes();
      } catch (error) {
        console.log(error);
      }
    },
    //获取初始化时候得值----主要针对类型切换时候需要换ui组件
    getInitValue() {
      this.ruleForm.dbName = this.$store.state.app.currentDBName;
      this.ruleForm.agent = this.$store.state.app.currentAgentID;
      this.ruleForm.name = this.$store.state.app.currentDSName;
      this.ruleForm.type = this.$store.state.app.currentDBType;
      this.ruleForm.resume = this.$store.state.app.currentResume;
      if (this.agentTypes.includes(this.ruleForm.type)) {
        this.showAgentSelect = true;
      } else {
        this.showAgentSelect = false;
      }
    },
    handleDbBtn() {
      this.$store.commit("dbs/HANDLE_ADD_DB");
      this.$store.commit("dbs/SET_ADD_DB_COMP", "datain");
      this.$store.commit("dbs/SET_DIALOG_DB_VISABLE", true);
    },
    //切换数据源
    changeDBType() {
      if (this.agentTypes.includes(this.ruleForm.type)) {
        this.showAgentSelect = true;
      } else {
        this.showAgentSelect = false;
      }
      this.$store.commit("app/SET_CURRENT_DBNAME", this.ruleForm.dbName);
      this.$store.commit("app/SET_CURRENT_AGENT", this.ruleForm.agent);
      this.$store.commit("app/SET_CURRENT_DSNAME", this.ruleForm.name);
      this.$store.commit("app/SET_CURRENT_DBTYPE", this.ruleForm.type);
    },
    getAgents() {
      this.agentList = this.$store.state.app.agentLists;
    },

    async downloadFile() {
      try {
        let result = await downlaodAllNodes();
        let blob = new Blob([result], { type: "text/csv,charset=UTF-8" });
        let link = document.createElement("a");
        link.download = "csv模板文件.csv";
        link.style.display = "none";
        link.href = URL.createObjectURL(blob);
        document.body.appendChild(link);
        link.click();
        URL.revokeObjectURL(link.href);
        document.body.removeChild(link);
      } catch (error) {
        console.log(error);
      }
    },
    createAgent() {
      this.$store.commit("app/SET_AGENT_DIALOG", true);
    },
    async getDbTypes() {
      try {
        this.dbTypes = await getUIData();
        if (!this.$store.state.app.currentDBType) {
          this.ruleForm.type = this.dbTypes[0].id;
        }
      } catch (error) {
        console.log(error);
      }
    },
    async getDBLists() {
      try {
        this.dbList = await getDBListReq();
      } catch (error) {
        console.log(error);
      }
    },
    async getAgentDataType() {
      try {
        this.agentList = await getAgentsData();
        this.agentList = this.agentList.map((agent) => {
          return {
            value: agent.id,
            label:
              agent.id +
              "." +
              agent.name +
              (new Date(agent.expire_date) < Date.now()
                ? "（" + this.$t("datasource.expired") + "）"
                : ""),
            disabled: new Date(agent.expire_date) < Date.now(),
            ...agent,
          };
        });
      } catch (error) {
        console.log(error);
      }
    },
    transforHtml(val) {
      return marked.parse(this.$t('dataIn.resumeTip'));
    },
  },
  watch: {
    "$i18n.locale":{
      deep:true,
      handler(val){
        this.getDbTypes()
      }
    },
    "$store.state.app.agentDialog": {
      handler(val) {
        this.showAgent = val;
      },
    },
    "$store.state.app.currentEditID": {
      deep: true,
      handler(val) {
        this.currentEditid = val;
      },
    },
    "$store.state.app.currentDBName": {
      deep: true,
      handler(val) {
        this.ruleForm.dbName = val;
      },
    },
    "$store.state.app.currentDBType": {
      deep: true,
      handler(val) {
        this.getInitValue();
      },
    },
    "$store.state.app.agentLists": {
      deep: true,
      handler(val) {
        this.agentList = val;
        this.ruleForm.agent = val[val.length - 1].id;
        this.$store.commit("app/SET_CURRENT_AGENT", this.ruleForm.agent);
      },
    },
  },
};
</script>
<style lang="scss">
.el-form-item__label {
  text-align: left;
  font-size: 14px;
  font-weight: 500;
  color: #4259ce;
  // position: relative;
  &::before {
    display: none;
  }
}
.el-form-item.is-required:not(.is-no-asterisk) > .el-form-item__label:after,
.el-form-item.is-required:not(.is-no-asterisk)
  .el-form-item__label-wrap
  > .el-form-item__label:after {
  content: "*";
  color: #ff4949;
  font-size: 12px;
  margin-left: 4px;
}
.el-button.ml {
  margin-left: 10px;
}
.custom-placeholder {
  color: #acaab2;
  font-size: 14px;
  margin-top: 10px;
}
.transverse{
  display:flex;
}
</style>
