<template>
  <div class="csv-parameter">
    <el-form
      :model="ruleForm"
      ref="ruleForm"
      label-width="200px"
      :rules="rules"
    >
      <el-form-item :label="$t('datasource.includeheader')" prop="hasHeader">
        <el-checkbox
          v-model="ruleForm.hasHeader"
          @change="changeHeader"
        ></el-checkbox>
      </el-form-item>
      <el-form-item
        :label="$t('datasource.customcolname')"
        prop="customcol"
        v-if="showcustom"
        required
        :rules="customcolrule"
      >
        <el-input v-model="ruleForm.customcol"></el-input>
      </el-form-item>
      <slot name="next"></slot>
      <el-form-item :label="$t('datasource.target')" prop="dbName">
        <el-select v-model="ruleForm.dbName" placeholder="">
          <el-option
            v-for="item in dblist"
            :key="item.name"
            :value="item.name"
            :label="item.name"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('datasource.name')" prop="subname">
        <el-input v-model="ruleForm.subname"></el-input>
      </el-form-item>
      <el-form-item :label="$t('datasource.using')" prop="tableName">
        <el-input v-model="ruleForm.tableName"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { getDBListReq } from "@/api/gateway/data/dbs.js";
export default {
  name: "CsvParameter",
  props: {
    targetName: {
      type: String,
      default: "",
    },
    isEditable: {
      type: Boolean,
      default: false,
    },
    echoData: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      showcustom: true,
      ruleForm: {
        hasHeader: false,
        dbName: "",
        subname: "",
        customcol: "",
        tableName: "",
        isValid: false,
      },
      customcolrule: [
        {
          required: true,
          trigger: "blur",
          message: this.$t("datasource.customcol"),
        },
      ],
      rules: {
        dbName: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.nametip"),
          },
        ],
        subname: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.name"),
          },
        ],

        tableName: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.tabletip"),
          },
        ],
      },
      dblist: [],
      timePercision: [
        {
          key: "ms",
          label: this.$t("datasource.ms"),
        },
        {
          key: "μs",
          label: this.$t("datasource.μs"),
        },
        {
          key: "ns",
          label: this.$t("datasource.ns"),
        },
      ],
    };
  },
  mounted() {
    if (this.isEditable) {
      this.ruleForm.dbName = this.targetName;
      this.ruleForm.subname=this.echoData[0].model.name
      this.ruleForm.tableName = this.echoData[0].model.using
      this.ruleForm.customcol= Object.keys(this.echoData[0].parse).join(',')
      this.ruleForm.hasHeader =
        this.$store.state.app.hasheader == "true" ? true : false;

      this.showcustom=!this.ruleForm.hasHeader
    }
    this.getDatabases();
  },
  methods: {
    changeHeader() {
      this.showcustom = !this.ruleForm.hasHeader;
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (error) {
        console.log(error);
      }
    },
    submit() {
      if (this.ruleForm.hasHeader) {
        this.rules;
      }
      this.$refs.ruleForm.validate((valid) => {
        if (valid) {
          this.isValid = true;
        } else {
          this.isValid = false;
          return false;
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.csv-parameter {
  ::v-deep {
    .el-form-item__label {
      margin-right: 20px;
    }
    .el-form-item {
      display: flex;
      white-space: nowrap;
    }
    .el-form-item__content {
      margin-left: 10px !important;
      flex: auto;
      display: flex;
      align-items: center;
    }
    .el-select {
      flex: 1;
    }
  }
}
</style>
