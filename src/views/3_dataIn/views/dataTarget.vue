<template>
  <div class="data-target">
    <el-form>
        <el-form-item label="名称">
            <el-input></el-input>
        </el-form-item>
        <el-form-item label="类型">

        </el-form-item>
        <el-form-item label="代理"></el-form-item>
        <el-form-item label="目标数据库"></el-form-item>
    </el-form>
  </div>
</template>
<script>
import { getUIData } from "@/api/explorer/datain";
export default {
  name: "DataTarget",
  data() {
    return {
        dbTypes:[]
    };
  },
  mounted(){
    this.getDbTypes()
  },
  methods:{
    async getDbTypes(){
        try {
        let data=await getUIData()
        console.log(data,'获取数据源类型');
        } catch (error) {
            console.log(error);
        }
    }
  }
};
</script>
