<template>
  <el-dialog
    align="center"
    :title="$t('datasource.addsource')"
    width="500px"
    :visible.sync="visible"
    :destroy-on-close="true"
    @closed="closeDialog"
  >
    <el-form
      :model="ruleForm"
      :rules="rules"
      ref="ruleForm"
      size="mini"
      label-width="auto"
      label-position="left"
      class="demo-ruleForm"
    >
      <el-form-item
        :label="$t('datasource.agent')"
        prop="agent"
        v-model="ruleForm.agent"
        :rules="{
          required: checked,
          message: $t('datasource.agenttip'),
          trigger: 'blur',
        }"
      >
        <span slot="label">
          <el-checkbox v-model="checked">
            {{ $t("datasource.agent") }}
          </el-checkbox>
          <el-tooltip effect="light" placement="top">
            <span slot="content" v-html="$t('datasource.agentInfo')"></span>
            <i class="el-icon-info"></i>
          </el-tooltip>
        </span>
        <!-- <el-cascader
          v-model="ruleForm.agent"
          :placeholder="$t('datasource.agenttip')"
          style="width: 100%"
          :options="options"
          :disabled="disabledAgent"
          >
        </el-cascader> -->
        <el-select
          v-model="ruleForm.agent"
          :placeholder="
            checked ? $t('datasource.agenttip') : this.$t('disbleagent')
          "
          :disabled="disabledAgent"
        >
          <el-option
            v-for="item in this.agentList"
            :key="item.value"
            :label="item.label"
            :value="item.value"
          >
          </el-option>
        </el-select>
        <el-tooltip :content="$t('taosagents.addagenttip')" effect="light" placement="top">
          <el-button type="primary" style="margin-left: 10px" @click="openAddAgentDialog">{{
            $t("taosagents.createnewagent")
          }}</el-button>
        </el-tooltip>
      </el-form-item>
      <el-form-item :label="$t('datasource.sourcetype')" prop="type">
        <el-select
          v-model="ruleForm.type"
          :placeholder="$t('datasource.typetip')"
        >
          <el-option
            :label="item.name"
            :value="item.id"
            v-for="item in originalTypes"
            :key="item.id"
          ></el-option>
        </el-select>
        <span
          style="color: red; font-size: 12px; display: flex; margin-top: 4px"
          v-if="ruleForm.type == 'influxdb'"
          >{{ $t("datasource.influxdbtip") }}</span
        >
      </el-form-item>
      <el-form-item :label="$t('datasource.sourcename')" prop="name">
        <el-input
          v-model="ruleForm.name"
          :placeholder="$t('datasource.nametip')"
          :maxlength="20"
        ></el-input>
      </el-form-item>
    </el-form>
    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button size="small" @click="closeDialog" class="w100">
          {{ $t("cancel") }}
        </el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button
          size="small"
          :disabled="confirmStatus"
          @click="handleAdd"
          class="w100"
          type="primary"
          >{{ $t("confirm") }}</el-button
        >
      </el-col>
    </el-row>
  </el-dialog>
</template>
<script>
import { getAgentsData } from "@/api/explorer/agent";
import { deepClone } from "@/utils";

export default {
  name: "AddDialog",
  props: {
    typeList: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.agent && this.checked) {
        return true;
      }
      if (!this.ruleForm.type) {
        return true;
      }
      if (!this.ruleForm.name) {
        return true;
      }
      return false;
    },
    options() {
      return [
        {
          value: "disableAgent",
          label: this.$t("disbleagent"),
        },
        {
          value: "start",
          label: this.$t("enableagent"),
          children: this.agentList,
        },
      ];
    },
    rules() {
      return {
        // agent: [
        //   {
        //     required: this.checked,
        //     message: this.$t("datasource.agenttip"),
        //   },
        // ],
        type: [
          {
            required: true,
            message: this.$t("datasource.typetip"),
          },
        ],
        name: [{ required: true, message: this.$t("datasource.nametip") }],
      };
    },
  },
  data() {
    return {
      visible: false,
      agentList: [],
      originalTypes: [],
      ruleForm: {
        agent: ["disableAgent"],
        type: "",
        name: "",
      },
      dataTypeMap: new Map([
        ["tmq", "TDengine Subscription"],
        ["pi", "PI"],
        ["opcda", "OPC-DA"],
        ["opcua", "OPC-UA"],
      ]),
      checked: false,
      disabledAgent: false,
      rules1: {
        agent: [
          {
            required: this.checked,
            message: this.$t("datasource.agenttip"),
          },
        ],
        type: [
          {
            required: true,
            message: this.$t("datasource.typetip"),
          },
        ],
        name: [{ required: true, message: this.$t("datasource.nametip") }],
      },
    };
  },
  mounted() {
    this.visible = true;
    this.getAgentDataType();
    this.originalTypes = deepClone(this.typeList);
  },
  methods: {
    openAddAgentDialog(){
      this.$parent.$refs.agents.add()
    },
    handleAdd() {
      localStorage.setItem("datainName", this.ruleForm.name);
      this.$parent.$parent.agentID = this.ruleForm.agent;
      this.$parent.$parent.toggleComponent(this.ruleForm.type, "", "", "");
    },

    selectAgenttype() {
      this.ruleForm.type = "";
      if (this.ruleForm.agent[0] === "add") {
        this.$emit("addAgent");
        this.$nextTick(() => {
          this.closeDialog();
        });
      }
    },

    closeDialog() {
      this.$refs.ruleForm.resetFields();
      // this.switchVal = false;
      // this.ruleForm.agent = "";
      this.$emit("closeDialog");
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
      } catch (error) {
        console.log(error);
      }
    },
  },
  watch: {
    "ruleForm.type": {
      deep: true,
      handler(val) {
        if (val == "mqtt") {
          //   this.$emit("showMqttDialog");
        }
      },
    },
    checked(val) {
      this.disabledAgent = !val;
      this.ruleForm.agent = "";
    },
  },
};
</script>
<style lang="scss" scoped>
.switch-agent {
  display: flex;
  margin-bottom: 15px;
  padding-left: 10px;
  .label {
    color: #4d6992;
    font-size: 16px;
    font-weight: 500;
  }
  ::v-deep {
    .el-switch__core {
      width: 55px !important;
    }
    .el-switch {
      margin-left: 25px;
    }
  }
}
::v-deep .el-checkbox__label {
  font-size: 16px;
  color: #4d6992;
}
.el-select {
  display: flex;
  flex: 1;
}
::v-deep {
  .el-form-item__content {
    display: flex;
  }
}
</style>
