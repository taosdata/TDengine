<template>
  <div class="csv-parameter">
    <el-form
      :model="ruleForm"
      ref="ruleForm"
      label-width="100px"
      :rules="rules"
    >
      <!-- <el-form-item :label="$t('datasource.includeheader')" prop="hasHeader">
        <el-checkbox
          v-model="ruleForm.hasHeader"
          @change="changeHeader"
          size="small"
        ></el-checkbox>
      </el-form-item>
      <el-form-item
        :label="$t('datasource.customcolname')"
        prop="customcol"
        v-if="showcustom"
        required
        :rules="customcolrule"
      >
        <el-input size="small" v-model="ruleForm.customcol"></el-input>
      </el-form-item> -->
    </el-form>
    <slot name="next"></slot>
    
  </div>
</template>
<script>
import { getDBListReq } from "@/api/gateway/data/dbs.js";
export default {
  name: "CsvParameter",
  props: {
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
      language:  localStorage.getItem('local_language'),
      showcustom: true,
      isAllValid: true,
      ruleForm: {
        hasHeader: false,
        customcol: "",
        isValid: false,
      },
      
      // customcolrule: [
      //   {
      //     required: true,
      //     trigger: "blur",
      //     message: this.$t("datasource.customcol"),
      //   },
      // ],
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
  computed:{
    customcolrule(){
      return {
        required: true,
          trigger: "blur",
          message: this.$t("datasource.customcol"),
      }
    }
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
    },
    "$i18n.locale": {
      deep: true,
      handler(val) {
        this.$nextTick(()=>{
          this.$refs.ruleForm.clearValidate();
        })
        
      },
    },
  },
  mounted() {
    if (this.isEditable) {
      // this.ruleForm2.subname = this.echoData[0].model.name;
      // this.ruleForm2.tableName = this.echoData[0].model.using;
      this.ruleForm.customcol = this.$store.state.app.csvTransformerlocalCols.length>0?this.$store.state.app.csvTransformerlocalCols.join(','):'';
      this.ruleForm.hasHeader =
        this.$store.state.app.hasheader == "true" ? true : false;
      // this.showStable = this.echoData[0].model?.tags?.length > 0 ? true : false;
      // this.$store.commit("SET_SHOW_CSV_STABLE", this.showStable);
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
      this.$store.commit("app/SET_CSV_HASHEADER", this.ruleForm.hasHeader);
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
        }
      });
      return this.isValid
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
      // margin-left: 10px !important;
      flex: auto;
      display: flex;
      align-items: center;
      span:first-child{
        flex:1;
      }
    }
    .el-select {
      flex: 1;
    }
  }
}
</style>
