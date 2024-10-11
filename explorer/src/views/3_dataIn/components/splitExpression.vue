<template>
  <div class="split-expression">
    <el-form :model="ruleForm" :rules="rules" ref="splitForm">
      <el-form-item prop="sep">
        <el-input
          placeholder=","
          class="split-item"
          size="small"
          v-model="ruleForm.sep"
          :disabled="isViewable"
        >
          <template slot="prepend">seperator</template>
        </el-input>
      </el-form-item>
      <el-form-item prop="n">
        <el-input
          placeholder="3"
          class="split-item"
          size="small"
          type="number"
          v-model="ruleForm.n"
          :disabled="isViewable"
        >
          <template slot="prepend">number</template>
        </el-input>
      </el-form-item>
      <el-form-item prop="names" style='display:none;'>
        <el-input
          placeholder="value1,value2,value3"
          class="split-item"
          size="small"
          v-model="ruleForm.names"
          :disabled="isViewable"
        >
          <template slot="prepend">names</template>
        </el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { deepClone } from "@/utils/index";
export default {
  name: "SplitExpression",
  inject: ['sourceParent'],
  props: {
    ruleForm: {
      type: Object,
      default: () => {
        return {
          sep: "",
          n: "",
          names: "",
        };
      },
    },
  },
  data() {
    return {
      isValid: true,
      // ruleForm: {
      //   sep: "",
      //   n: "",
      //   names: "",
      // },
      rules: {
        n:[
        {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.sepntip"),
          },
        ],
        sep: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.septip"),
          },
        ],
      },
    };
  },
  methods: {
    submit() {
      this.$refs.splitForm.validate((valid) => {
        if (valid) {
          this.isValid = true;
          let splitExpre = {};
          Object.keys(this.ruleForm)
            .filter((key) => this.ruleForm[key])
            .forEach((item) => {
              splitExpre[item] =
                item == "names"
                  ? this.ruleForm[item].toString()
                  : item == "n"
                  ? Number(this.ruleForm[item])
                  : this.ruleForm[item].toString().trim();
            });
          if (splitExpre.names) {
            let result = splitExpre.names
              .toString()
              .split(",")
              .map((val) => val.trim());
            splitExpre.names = result;
          }
          this.$store.commit("app/SET_SPLIT_EXPRESS", splitExpre);
          return true;
        } else {
          this.isValid = false;
          return false;
        }
      });
    },
  },
  mounted() {
    if (this.$store.state.app.splitExpresList) {
      let middleobj = deepClone(this.$store.state.app.splitExpresList);
      if (
        this.$store.state.app.splitExpresList.names &&
        Array.isArray(this.$store.state.app.splitExpresList.names)
      ) {
        middleobj.names =
          this.$store.state.app.splitExpresList.names.toString();
      }
      // this.ruleForm = { ...middleobj };
    }
  },
  watch: {
    "$store.state.app.splitExpresList": {
      deep: true,
      handler(val) {
        let middleObj = deepClone(val);
        if (val.names && Array.isArray(val.names)) {
          middleObj.names = middleObj.names.toString();
        }
        // this.ruleForm = { ...middleObj };
      },
    },
  },
  computed: {
    isViewable() {
      return this.sourceParent.isViewable;
    },
  }
};
</script>
<style lang="scss" scoped>
.split-expression {
  .el-form {
    display: grid;
    grid-template-columns: 1fr 1fr ;
    column-gap: 0px !important;
    ::v-deep {
      .el-input-group__prepend {
        padding: 0px 4px !important;
        border-radius: 0px !important;
      }
      .el-input__inner {
        padding: 0 0 0 4px !important;
        border-radius: 0px !important;
        border: 1px solid #e3e3e3 !important;
      }
    }
  }
  .el-form-item {
    .split-item {
      border: none !important;
    }
    &:not(:last-child) {
      ::v-deep {
        .el-input__inner {
          border-right: none !important;
        }
      }
    }
  }
}
</style>
