<template>
  <div class="split-expression">
    <el-form :model="ruleForm" :rules="rules" ref="splitForm">
      <el-form-item prop="sep">
        <el-input
          placeholder=","
          class="split-item"
          size="small"
          v-model="ruleForm.sep"
        >
          <template slot="prepend">sep</template>
        </el-input>
      </el-form-item>
      <el-form-item prop="n">
        <el-input
          placeholder="3"
          class="split-item"
          size="small"
          type="number"
          v-model="ruleForm.n"
        >
          <template slot="prepend">n</template>
        </el-input>
      </el-form-item>
      <el-form-item prop="names">
        <el-input
          placeholder='value1,value2,value3'
          class="split-item"
          size="small"
          v-model="ruleForm.names"
        >
          <template slot="prepend">names</template>
        </el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
export default {
  name: "SplitExpression",
  data() {
    return {
      isValid: true,
      ruleForm: {
        sep: "",
        n: "",
        names: '',
      },
      rules: {
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
          let splitExpre={}
          Object.keys(this.ruleForm)
            .filter((key) => this.ruleForm[key])
            .forEach((item) => {
              splitExpre[item] =item=='names'?this.ruleForm[item].toString():item=='n'?Number(this.ruleForm[item]) :this.ruleForm[item];

              
            });
            if(splitExpre.names){
                let result = splitExpre.names.toString().split(',')
                splitExpre.names=result


                console.log('有names',splitExpre);
            }
            this.$store.commit('app/SET_SPLIT_EXPRESS',splitExpre)
            console.log(this.$store.state.app.splitExpresList,splitExpre,'split---store');
          return true;
        } else {
          this.isValid = false;
          return false;
        }
      });
    },
  },
  mounted(){
    if(this.$store.state.app.splitExpresList){
        if(this.$store.state.app.splitExpresList.names){
            this.$store.state.app.splitExpresList.names=this.$store.state.app.splitExpresList.names.toString()
            }
        this.ruleForm={...this.$store.state.app.splitExpresList}
    }
  },
  watch:{
    "$store.state.app.splitExpresList":{
        deep:true,
        handler(val){
            console.log(val,'split监听');
            if(val.names){
                val.names=val.names.toString()
            }
            this.ruleForm={...val}
        }
    }
  }
};
</script>
<style lang="scss" scoped>
.split-expression {
  .el-form {
    display: grid;
    grid-template-columns: 1fr 1fr 3fr;
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
