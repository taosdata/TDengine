<template>
  <div class="opc-connector">
    <ul class="singleton-header">
      <li>
        <span>
          {{ $t("datasource.primarykey") }}
        </span>
        <el-tooltip
          effect="light"
          :content="$t('datasource.primarytip')"
          placement="right-start"
        >
          <i class="el-icon-info"></i>
        </el-tooltip>
      </li>
      <li>
        <span>{{ $t("datasource.warehousing") }}</span>
      </li>
      <li v-for="(item, index) in headers" :key="index">
        <span>
          {{ $t(`datasource.${item}`) }}
        </span>
      </li>
    </ul>
    <ul
      class="singleton-cols"
      v-for="(item, index) in opcConfig.column_configs"
      :key="index"
    >
      <li>
        <el-checkbox
          :disabled="item.column_type != 'timestamp'"
          @change="changePrimary(item)"
          :value="item.column_name == currentPrimary"
        ></el-checkbox>
      </li>
      <li>
        <el-checkbox
          @change="saveToDb(item)"
          :value="saveFileds.includes(item.column_name)"
          :checked="item.column_name == currentPrimary"
        ></el-checkbox>
      </li>
      <li>
        <span>{{ item.column_name }}</span>
      </li>
      <li>
        <el-input v-model="item.column_alias" size="mini"></el-input>
      </li>
      <li>
        <span>{{ item.column_type }}</span>
      </li>
    </ul>
    <el-button @click="submit">确定</el-button>

    <el-form
      :model="ruleForm"
      :rules="rules"
      ref="ruleForm"
      style="width: 80%; margin-top: 20px"
      label-width="100px"
    >
      <el-form-item
        :label="$t('datasource.stable_prefix')"
        prop="stable_prefix"
      >
        <el-input v-model="ruleForm.stable_prefix"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { Message } from "element-ui";

export default {
  name: "OpcConnector",
  props: {
    opcConfig: {
      type: Object,
      default: () => {
        return null;
      },
    },
  },
  data() {
    return {
      isReject:false,
      ruleForm: {
        stable_prefix: "",
      },
      rules: {
        stable_prefix: [
          {
            required: true,
            message: this.$t("datasource.stable_prefixtip"),
          },
        ],
      },
      currentPrimary: "received_time",
      //   opcConfig,
      headers: ["colname", "rename", "coltype"],
      saveFileds: ["value", "received_time"],
    };
  },
  mounted() {
    console.log(this.opcConfig, "opcConfig");
  },
  methods: {
    saveToDb(val) {
      if (!this.saveFileds.includes(val.column_name)) {
        this.saveFileds.push(val.column_name);
      } else {
        if (
          val.column_name == this.currentPrimary ||
          val.column_name == "value"
        ) {
          Message.warning(this.$t('datasource.primaryvaluetip'));
          return;
        }
        let index = this.saveFileds.indexOf(val.column_name);
        this.saveFileds.splice(index, 1);
      }
      console.log(val, "xuyao 入库的字段");
    },
    changePrimary(val) {
      this.currentPrimary = val.column_name;
      if (!this.saveFileds.includes(val.column_name)) {
        //主键列一定会入库
        this.saveFileds.push(val.column_name);
      }
      console.log(val, "change", this.currentPrimary);
    },
    structureData() {},
    submit() {
      let savedata = this.opcConfig.column_configs.filter((item) => {
        return this.saveFileds.includes(item.column_name)
      }).map(val=>{
        if(val.column_name==this.currentPrimary){
            val['is_primary_key']=true
        }else{
            val['is_primary_key']=false
        }
        return val
      });

      if(!this.ruleForm.stable_prefix){
        this.isReject=true
        return
      }else{
        this.isReject=false
      }
      this.$store.commit('app/SET_OPC_CONFIG',{
        column_configs:savedata,
        stable_prefix:this.ruleForm.stable_prefix
      })
      console.log(this.$store.state.app.opcConfig,this.isReject, savedata,this.opcConfig, this.saveFileds, "submit");
    },
  },
};
</script>
<style lang="scss" scoped>
.opc-connector {
  display: flex;
  flex-direction: column;
}
.singleton-header {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr 1fr;
  column-gap: 10px;
  border-top: 1px solid #ebeef5;
  padding-top: 8px;
  padding-bottom: 8px;
  width: 100%;
  background: #f5f7fa;
  li {
    display: flex;
    justify-content: center;
    align-content: center;
  }
}
.singleton-cols {
  display: grid;
  width: 100%;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr 1fr;
  border-bottom: 1px solid #ebeef5;
  border-top: none;
  li {
    display: flex;
    justify-content: center;
    align-content: center;
    // border-top: 1px solid #ebeef5;
    padding-top: 8px;
    padding-bottom: 8px;
  }
}
</style>