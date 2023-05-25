<template>
  <el-dialog
    align="center"
    :title="$t('datasource.mqtttitle')"
    width="900px"
    :visible.sync="visible"
    :destroy-on-close="true"
    @closed="closeMqttDialog"
  >
    <div class="connector">
      <div class="json-zone">
        <ul class="header">
          <li>{{ $t("datasource.colname") }}</li>
          <li>{{ $t("datasource.rename") }}</li>
          <li>{{ $t("datasource.type") }}</li>
          <li></li>
        </ul>
        <span class="info">
          <el-tooltip
            class="item"
            effect="light"
            :content="$t('datasource.addmqtttip')"
            placement="top-start"
          >
            <i class="el-icon-info"></i>
          </el-tooltip>
        </span>
        <div class="col-content">
          <Mqttcolumn
            v-for="(item, index) in columnNum"
            :key="index"
            :index="index"
            @deleteRow="deleteRow"
            @changeAddStatus="changeAddStatus"
            @sendLatestCont="getLatestCont"
            ref="mqtt"
          >
          </Mqttcolumn>
        </div>
        <el-button
          icon="el-icon-plus"
          size="small"
          type="primary"
          :disabled="!disable"
          plain
          @click="addRow"
        ></el-button>
      </div>
      <el-form
        label-width="110px"
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
      >
        <el-form-item :label="$t('datasource.subname')" prop="subtableName">
          <el-input v-model="ruleForm.subtableName" size="mini"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('datasource.supertable')"
          prop="supertableName"
        >
          <el-input v-model="ruleForm.supertableName" size="mini"></el-input>
        </el-form-item>
        <el-form-item label="tags：" prop="tagsName">
          <el-input v-model="ruleForm.tagsName" size="mini"></el-input>
        </el-form-item>
      </el-form>
      <div class="footer">
        <el-button size="small" style="width: 100px" @click="closeMqttDialog">{{
          $t("datasource.cancel")
        }}</el-button>
        <el-button
          type="primary"
          size="small"
          style="width: 100px"
          :disabled="confirmStatus"
          @click="getMqttParser"
          >{{ $t("datasource.ok") }}</el-button
        >
      </div>
    </div>
  </el-dialog>
</template>
<script>
import Mqttcolumn from "./mqttColumn.vue";
export default {
  name: "MqttConnector",
  components: { Mqttcolumn },
  data() {
    return {
      visible: true,
      columnNum: [
        {
          column: "",
          alias: "",
          type: "",
        },
      ],
      disable: false,
      ruleForm: {
        subtableName: "",
        supertableName: "",
        tagsName: "",
      },
      rules: {
        subtableName: [
          {
            required: true,
            message: this.$t("datasource.subtip"),
            trigger: "blur",
          },
        ],
        supertableName: [
          {
            required: true,
            message: this.$t("datasource.supertip"),
            trigger: "blur",
          },
        ],
        tagsName: [
          {
            required: true,
            message: this.$t("datasource.tagtip"),
            trigger: "blur",
          },
        ],
      },
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.subtableName) {
        return true;
      }
      if (!this.ruleForm.supertableName) {
        return true;
      }
      if (!this.ruleForm.tagsName) {
        return true;
      }
      if (!this.disable) {
        return true;
      }
      return false;
    },
  },
  methods: {
    getLatestCont(cont, index) {
      this.$set(this.columnNum, index, cont);

      console.log(cont, index, "获取的内容", this.columnNum);
    },
    changeAddStatus() {
      this.$nextTick(() => {
        this.disable = Array.from(this.$refs.mqtt).every(
          (item) => !item.addStatus
        );
        console.log(this.$refs.mqtt, this.disable);
      });
    },
    deleteRow(ind) {
      this.$confirm(this.$t("datasource.delcol"), {
        confirmButtonText: this.$t("datasource.ok"),
        cancelButtonText: this.$t("datasource.cancel"),
        type: "warning",
      }).then(() => {
        this.columnNum.splice(ind, 1);
      });

      console.log(ind, "要删除的索引位置", this.columnNum);
    },
    addRow() {
      this.columnNum.push({ column: "", alias: "", type: "" });
      this.changeAddStatus();
    },
    closeMqttDialog() {
      this.$emit("closeMqttDialog");
    },

    getMqttParser() {
      this.$parent.$parent.mqttParser = {
        parse: {
          payload: {
            json: this.columnNum,
            keep: true,
          }
        },

        model: {
          name: this.ruleForm.subtableName,
          using: this.ruleForm.supertableName,
          tags: [].concat(this.ruleForm.tagsName.split(",")),
        },
      };

      this.$emit("closeMqttDialog");
      console.log(this.ruleForm, this.columnNum, "获取参数body", this);
    },
  },
};
</script>
<style lang="scss" scoped>
.connector {
  margin-top: 15px;
}
.json-zone {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  max-height: 200px;
  padding-left: 110px;
  margin-bottom: 15px;
  position: relative;
  .header {
    display: grid;
    grid-template-columns: 2fr 2fr 2fr 1fr;
    margin-bottom: 15px;
    li {
      color: #4d6992;
      font-size: 16px;
    }
  }
  .col-content {
    overflow: auto;
    border: none;
    margin-bottom: 15px;
  }
  .info {
    position: absolute;
    top: 38px;
    left: 70px;
    cursor: pointer;
    i {
      font-size: 25px;
      color: #4d6992;
    }
  }
}
</style>