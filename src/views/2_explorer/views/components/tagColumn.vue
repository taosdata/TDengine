<template>
  <div class="tag-column">
    <span class="label">{{tagColumnData.field}}</span>
    <el-select :value="tagColumnData.condition" placeholder="" size="small" @change="setCondition" style="width:200px;">
      <el-option
        v-for="item in tagColumnData.conditionList"
        :key="item"
        :label="item"
        :value="item"  
      >
      </el-option>
    </el-select>
    <el-input :value="tagColumnData.value" size="small" @input="setTagValue"></el-input>
  </div>
</template>
<script>



export default {
  name: "TagColumn",
  props:{
    tagColumnData:{
        type:Object,
        default:()=>{
            return null
        }
    }
  },
  data() {
    return {
        
    };
  },
  methods:{
    setCondition(val){
        this.tagColumnData.condition=val
        this.$forceUpdate()
    },
    setTagValue(val){
        this.tagColumnData.value=val
        this.$forceUpdate()
    }
  }
};
</script>
<style lang="scss" scoped>
.tag-column{
    margin-bottom:15px;
    display:grid;
    grid-template-columns: 1fr 2fr 3fr;
    .label{
        text-align: center;
        line-height: 30px;
        margin-right:0px;
    }
}
</style>
