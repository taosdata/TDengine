<template>
  <div class="csv-parameter">
    <el-form
      :model="ruleForm"
      ref="ruleForm"
      :label-width="language.includes('en') ? '200px' : '120px'"
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
    </el-form>
    <slot name="next"></slot>
    <el-form
      :model="ruleForm2"
      ref="ruleForm2"
      :label-width="language.includes('en') ? '200px' : '120px'"
      :rules="rules"
    >

      <el-form-item
        :label="
          showStable ? $t('datasource.name') : $t('datasource.normalname')
        "
        prop="subname"
        :rules="showStable ? rules.subname : normaltable"
      >

        <el-input v-model="ruleForm2.subname"></el-input>
      </el-form-item>
      <el-form-item
        :label="$t('datasource.using')"
        prop="tableName"
        v-if="showStable"
        :rules="tableName"
      >
        <el-input v-model="ruleForm2.tableName"></el-input>
      </el-form-item>
      <!-- <el-form-item :label="$t('datasource.target')" prop="dbName">
        <el-select v-model="ruleForm2.dbName" placeholder="">
          <el-option
            v-for="item in dblist"
            :key="item.name"
            :value="item.name"
            :label="item.name"
          ></el-option>
        </el-select>
      </el-form-item> -->
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
      showStable: false,
      language: window.navigator.language,
      showcustom: true,
      isAllValid: true,
      ruleForm: {
        hasHeader: false,
        customcol: "",
        isValid: false,
      },
      ruleForm2: {
        // dbName: "",
        subname: "",
        tableName: "",
      },
      customcolrule: [
        {
          required: true,
          trigger: "blur",
          message: this.$t("datasource.customcol"),
        },
      ],
      tableName: [
        {
          required: true,
          trigger: "blur",
          message: this.$t("datasource.tabletip"),
        },
      ],
      normaltable: [
        {
          required: true,
          trigger: "blur",
          message: this.$t("datasource.normalname"),
        },
      ],
      rules: {
        // dbName: [
        //   {
        //     required: true,
        //     trigger: "change",
        //     message: this.$t("datasource.nametip"),
        //   },
        // ],
        subname: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.name"),
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
  watch: {
    "$store.state.app.showcsvStable": {
      deep: true,
      handler(val) {
        this.showStable = val;
      },
    },
    "$store.state.dbs.dialogDbVisible": {
      handler(val) {
        if (!val) {
          this.getDatabases()
        }
      }
    }
  },
  mounted() {
    if (this.isEditable) {
      // this.ruleForm2.dbName = this.targetName;
      this.ruleForm2.subname = this.echoData[0].model.name;
      this.ruleForm2.tableName = this.echoData[0].model.using;
      this.ruleForm.customcol = Object.keys(this.echoData[0].parse).join(",");
      this.ruleForm.hasHeader =
        this.$store.state.app.hasheader == "true" ? true : false;
      this.showStable = this.echoData[0].model?.tags?.length > 0 ? true : false;
      this.$store.commit("SET_SHOW_CSV_STABLE", this.showStable);
      this.showcustom = !this.ruleForm.hasHeader;
    }
    this.getDatabases();
  },
  methods: {
    handleDbBtn() {
      this.$store.commit("dbs/HANDLE_ADD_DB");
      this.$store.commit("dbs/SET_ADD_DB_COMP",'datain');
      this.$store.commit('dbs/SET_DIALOG_DB_VISABLE', true)
    },
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
      this.$refs.ruleForm.validate((valid) => {
        if (valid) {
          this.isValid = true;
        } else {
          this.isValid = false;
          return false;
        }
      });
    },
    submit2() {
      this.$refs.ruleForm2.validate((valid) => {
        if (valid) {
          this.isAllValid = true;
        } else {
          this.isAllValid = false;
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
      font-size: 14px;
      color:#4259ce;
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
