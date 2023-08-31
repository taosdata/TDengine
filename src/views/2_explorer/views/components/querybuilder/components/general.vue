<template>
  <div>
    <el-row class="row-style">
      <el-col :span="12" class="col-style">
        <span class="label">{{ $t('database') }}</span>
        <el-select 
          v-model="general.dbname" 
          placeholder="" 
          style="margin-right: 8px;" 
          :size="size"
          @change="dbChange"
          >
          <el-option
            v-for="db in dblist"
            :key="db['node-key']"
            :label="db.name"
            :value="db.name"
          ></el-option>
        </el-select>
      </el-col>
      <el-col :span="12" class="col-style">
        <span class="label">{{ $t('dashboard.tables') }}</span>
        <!-- <el-cascader
          class="tb-cascader"
          v-model="general.tbName" 
          :options="tableData"
          :props="props"
          collapse-tags
          clearable
          filterable
          :size="size"
          @change="handleCascader"
          ></el-cascader> -->
        <el-input
          v-model="general.tbName"
          :size="size"
          style="margin-right: 8px;"
          @blur="handleBlur"
        ></el-input>
        </el-col>
    </el-row>
    <el-row>
      <el-col :span="12" class="col-style">
        <span class="label">SELECT</span>
        <!-- <el-select v-model="general.fields" placeholder="" style="margin-right: 8px;" size="size" multiple>
          <el-option
            v-for="db in fieldData"
            :key="db.field"
            :label="db.field"
            :value="db.name"
          ></el-option>
        </el-select>; -->
        <el-input
          v-model="general.fields"
          :size="size"
          style="margin-right: 8px;"
        ></el-input>
      </el-col>
      <el-col :span="12" class="col-style">
        <span class="label">FROM</span>
        {{ fromVal }}
      </el-col>
    </el-row>
  </div>
</template>

<script>
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import {
  getStableListReq,
  getAllNormalTables,
  getStableStructReq,
} from "@/api/gateway/data/stables.js";
import {
  getTableListReq,
  getMatrixStructReq,
  getTableStructReq,
} from "@/api/gateway/data/tables.js";


  export default {
    name:'General',
    data() {
      return {
        dblist: [],
        pageSize: 100,
        // options: []
        props: { 
          lazy: true, 
          label: 'name',
          value: 'name',
          checkStrictly: true,
          lazyLoad: this.loadNode
        },
        tableData: [],
        fieldData: []
      }
    },
    props: {
      general: {
        type: Object,
        default: () => {}
      },
      size: {
        type: String,
        default: 'mini'
      }
    },
    emits: ['getFromVal'],
    computed: {
      // tbName() {
      //   return this.general.tbName.length <= 2 
      //     ? this.general.tbName[1] 
      //     : this.general.tbName[2]
      // },
      fromVal() {
        // const result =  `${this.general.dbname}.${this.general.tbName.length <= 2 
        //   ? this.general.tbName[1] || '' 
        //   : this.general.tbName[2] || ''}`
        const dbname = this.general.dbname ? `\`${this.general.dbname}\`` : ''
        const tbname = this.general.tbName ? `.\`${this.general.tbName}\``: ''
        const result = dbname + tbname
        this.getFromVal(result)
        return result
      }
    },
    mounted() {
      this.getDatabases()
    },
    methods: {
      getFromVal (val) {
        this.$emit('getFromVal', val)
      },
      async getDatabases() {
        try {
          this.dblist = await getDBListReq();
        } catch (error) {
          console.log(error);
        }
      },
      async dbChange(val) {
       this.tableData = [
        {
          value: 'STables',
          name: 'STables',
          level: 0,
          typeName: 'STables'
        },
        {
          value: 'Tables',
          name: 'Tables',
          level: 0,
          typeName: 'Tables'
        }]
      },
      async loadNode(node, resolve) {
        let data = node.data;
        let nodes = []
        switch (node.data?.typeName) {
          case "STables": 
            nodes = await getStableListReq(
              {
                pageSize: this.pageSize,
                currentPage: 1,
              },
              this.general.dbname
              )
              nodes = nodes[0]
              .map(item => ({
                ...item,
                leaf: node.leaf + 1,
                level: node.level + 1
              }));
            return resolve(nodes);
           
          case "Tables": 
            nodes = await getAllNormalTables(
                {
                  pageSize: this.pageSize,
                  currentPage: 1,
                },
                this.general.dbname
              )
          
            nodes = nodes[0]
              .map(item => ({
                ...item,
                leaf: 1,
                level: node.level + 1
              }));
            return resolve(nodes);
          case "stable":
             nodes = await getTableListReq({
                selected_stb: data.name,
                pageSize: this.pageSize,
                currentPage: 1,
                selected_db: this.general.dbname,
              })
            nodes = nodes[0]
              .map(item => ({
                ...item,
                leaf: 2,
                level: node.level + 1
              }));
            return resolve(nodes);
        }
      },
      async handleCascader(val) {
        let res = await getMatrixStructReq({
          selected_db: this.general.dbname,
          selected_tb: this.tbName,
        })
        this.fieldData = res 
        this.$store.commit('console/SET_FIELEDS',res)
      },
      async handleBlur(val) {
        if (this.general.dbname && this.general.tbName) {
          let res = await getMatrixStructReq({
            selected_db: this.general.dbname,
            selected_tb: this.general.tbName,
          })
          this.fieldData = res 
          this.$store.commit('console/SET_FIELEDS',res)
        }
      }
    }
  }
</script>
<style scoped lang="scss">
.col-style {
  display: flex;
  align-items: center;
  & >div {
    width: 240px;
  }
}
.row-style {
  margin-bottom: 8px;
}

</style>