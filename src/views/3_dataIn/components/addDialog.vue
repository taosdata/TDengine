<template>
  <el-dialog
    align="center"
    :title="$t('datasource.addsource')"
    width="400px"
    :visible.sync="visible"
    :destroy-on-close="true"
    @closed="closeDialog"
  >
    <div class="switch-agent" v-if="agentList.length > 0">
      <span class="label">{{ $t("enableagent") }}</span>
      <el-switch v-model="switchVal" @change="changeAgent"></el-switch>
    </div>

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
        required
        v-if="switchVal"
      >
        <el-select
          v-model="ruleForm.agent"
          :placeholder="$t('datasource.agenttip')"
          @change="selectAgenttype"
        >
          <el-option
            :label="`${item.id}. ${item.name}`"
            :value="item.id"
            v-for="item in agentList"
            :key="item.id"
          ></el-option>
        </el-select>
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
      </el-form-item>
      <el-form-item :label="$t('datasource.sourcename')" prop="name">
        <el-input
          v-model="ruleForm.name"
          :placeholder="$t('datasource.nametip')"
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
      if (!this.ruleForm.type) {
        return true;
      }
      if (!this.ruleForm.name) {
        return true;
      }
      if (!this.ruleForm.agent && this.switchVal) {
        return true;
      }
      return false;
    },
  },
  data() {
    return {
      visible: true,
      agentList: [],
      switchVal: false,
      originalTypes: [],
      ruleForm: {
        agent: "",
        type: "",
        name: "",
      },
      rules: {
        agent: [
          {
            required: true,
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
      dataTypeMap: new Map([
        ["tmq", "TDengine Subscription"],
        ["pi", "PI"],
        ["opcda", "OPC-DA"],
        ["opcua", "OPC-UA"],
      ]),
    };
  },
  mounted() {
    this.getAgentDataType();
    this.originalTypes = deepClone(this.typeList);
  },
  methods: {
    handleAdd() {
      localStorage.setItem("datainName", this.ruleForm.name);
      this.$parent.$parent.agentID = this.ruleForm.agent;
      this.$parent.$parent.toggleComponent(this.ruleForm.type, "", "", "");
    },
    selectAgenttype() {
      this.ruleForm.type = "";
      this.originalTypes = deepClone(
        this.agentList
          .filter((item) => item.id == this.ruleForm.agent)[0]
          .connectors.map((val) => {
            return {
              id: val,
              name: this.dataTypeMap.get(val),
            };
          })
      );
    },
    closeDialog() {
      this.$refs.ruleForm.resetFields();
      // this.switchVal = false;
      // this.ruleForm.agent = "";
      this.$emit("closeDialog");
    },
    changeAgent() {
      console.log(this.switchVal, "是否启用代理", this.typeList);
      this.$refs.ruleForm.resetFields();
      if (this.switchVal) {
        this.getAgentDataType();
      } else {
        this.originalTypes = deepClone(this.typeList);
      }
    },
    async getAgentDataType() {
      try {
        this.agentList = await getAgentsData(
          localStorage.getItem("local_clusterID"),
          localStorage.getItem("username")
        );
      } catch (error) {
        console.log(error);
      }
    },
  },
  watch: {
    "ruleForm.type": {
      deep: true,
      handler(val) {
        console.log(val, "舰艇类型");
        if (val == "mqtt") {
          this.$emit("showMqttDialog");
        }
      },
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
.el-select {
  display: flex;
  flex: 1;
}
</style>