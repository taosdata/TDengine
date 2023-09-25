<template>
  <div class="data-target">
    <el-form :model="ruleForm" ref="ruleForm">
      <el-form-item label="名称" prop="name">
        <el-input></el-input>
      </el-form-item>
      <el-form-item label="类型" prop="type">
        <el-select v-model="ruleForm.type">
          <el-option
            v-for="item in dbTypes"
            :label="item.name"
            :key="item.id"
            :value="item.id"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="代理" prop="'agent'">
        <el-select v-model="ruleForm.agent">
          <el-option
            v-for="item in agentList"
            :label="item.name"
            :key="item.id"
            :value="item.id"
          ></el-option>
        </el-select>
        <el-button type="primary" @click="createAgent">新增代理</el-button>
      </el-form-item>
      <el-form-item label="目标数据库" prop="dbName"></el-form-item>
    </el-form>
    <el-dialog
      align="center"
      title="新增代理"
      width="600px"
      :visible.sync="showAgent"
      :destroy-on-close="true"
    >
      <el-form
        :model="ruleAgentForm"
        :rules="rulesAgent"
        ref="ruleForm"
        size="mini"
        label-width="120px"
        class="demo-ruleForm"
      >
        <el-form-item prop="name" :label="$t('taosagents.name')">
          <el-input
            v-model.trim="ruleAgentForm.name"
            :maxlength="20"
          ></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            class="w100"
            type="primary"
            @click="addAgent"
            >{{ $t("confirm") }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script>
import { getUIData } from "@/api/explorer/datain";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { getAgentsData, addNewAgent } from "@/api/explorer/agent";

import Agents from "../components/agents.vue";
import { Message } from "element-ui";
export default {
  name: "DataTarget",
  components: { Agents },
  data() {
    return {
      dbTypes: [],
      dbList: [],
      agentList: [],
      showAgent: false,
      ruleForm: {
        name: "",
        type: "",
        agent: "",
        dbName: "",
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
    //编辑状态需要调用，新增不需要在mounted中调用
    // this.getAgentDataType();
  },
  methods: {
    createAgent() {
      this.showAgent = true;
    },
    async addAgent() {
      try {
        let params = {
          cluster_id: localStorage.getItem("local_clusterID"),
          dsn: localStorage.getItem("base_url"),
          name: this.ruleAgentForm.name,
          user_id: localStorage.getItem("username"),
        };
        let res = await addNewAgent(params);
        if(res.message){
            Message.error(res.message)
            return
        }
        this.getAgentDataType()
        console.log(res,'新增代理');
      } catch (error) {
        console.log(error);
      }
    },
    async getDbTypes() {
      try {
        this.dbTypes = await getUIData();
        console.log(this.dbTypes, "获取数据源类型");
      } catch (error) {
        console.log(error);
      }
    },
    async getDBLists() {
      try {
        this.dbList = await getDBListReq();
        console.log(this.dbList, "查询的数据库");
      } catch (error) {
        console.log(error);
      }
    },
    async getAgentDataType() {
      try {
        this.agentList = await getAgentsData(
          localStorage.getItem("local_clusterID"),
          localStorage.getItem("username")
        );
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
        console.log(this.agentList, "this.agentListthis.agentList");
      } catch (error) {
        console.log(error);
      }
    },
  },
};
</script>
