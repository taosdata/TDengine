<template>
  <div class="csv-parameter">
    <el-form :model="ruleForm" ref="ruleForm" label-width="100px" :rules="rules">
      <el-form-item :label="$t('datasource.includeheader')" prop="hasHeader">
        <el-checkbox v-model="ruleForm.hasHeader"></el-checkbox>
      </el-form-item>
      <el-form-item :label="$t('datasource.target')" prop="dbName">
        <el-select v-model="ruleForm.dbName" placeholder="">
          <el-option v-for="item in dblist" :key="item.name" :value="item.name" :label="item.name"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('datasource.percision')" prop="percision">
        <el-select v-model="ruleForm.percision" placeholder="">
          <el-option v-for="item in timePercision" :key="item.key" :value="item.key" :label="item.label"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('datasource.csvtable')" prop="tableName">
        <el-input v-model="ruleForm.tableName"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { getDBListReq } from "@/api/gateway/data/dbs.js";
export default {
  name: "CsvParameter",
  data() {
    return {
      ruleForm: {
        hasHeader: false,
        dbName: "",
        percision: "",
        tableName: "",
        isValid:false
      },
      rules:{
        dbName:[
          {
            required:true,trigger:'change',message:this.$t('datasource.nametip')
          }
        ],
        percision:[
          {
            required:true,trigger:'change',message:this.$t('datasource.percisiontip')
          }
        ],
        tableName:[
          {
            required:true,trigger:'blur',message:this.$t('datasource.tabletip')
          }
        ]
      },
      dblist:[],
      timePercision:[
        {
          key:'ms',
          label:this.$t('datasource.ms')
        },
        {
          key:'μs',
          label:this.$t('datasource.μs')
        },
        {
          key:'ns',
          label:this.$t('datasource.ns')
        }
      ]
    };
  },
  mounted(){
    this.getDatabases()
  },
  methods:{
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
        console.log(this.dblist);
      } catch (error) {
        console.log(error);
      }
    },
    submit(){
       this.$refs.ruleForm.validate((valid) => {
          if (valid) {
            this.isValid=true
          } else {
            this.isValid=false
            return false;
          }
        });
    }
  }
};
</script>
<style lang="scss" scoped>
.csv-parameter {
  width: 500px;
  ::v-deep {
    .el-form-item__label {
      margin-right: 20px;
    }
    .el-form-item {
      display: flex;
      white-space: nowrap;
    }
    .el-form-item__content{
      margin-left: 10px!important;
      flex: auto;
      display: flex;
      align-items: center;
    }
    .el-select{
      flex: 1;
    }
  }
}
</style>
