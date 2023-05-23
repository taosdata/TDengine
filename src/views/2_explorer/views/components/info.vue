<template>
  <div class="info">
    <el-form label-width="180px" label-position="right">
      <section class="info-content">
        <section class="left">
          <el-form-item
            v-for="item in leftField"
            :key="item"
            :label="item + ':'"
          >
            {{ infoData[item] }}
          </el-form-item>
          <el-form-item v-if="infoType !== 'database'" :label="infoData.stable_name?'tags:':'columns:'">
            <el-table
              tooltip-effect="light"
              style="width: 80%"
              size="mini"
              :data="tags"
            >
              <el-table-column
                :show-overflow-tooltip="true"
                min-width="100"
                label="name"
                prop="name"
              >
              </el-table-column>
              
              <el-table-column
                :show-overflow-tooltip="true"
                :label="infoType=='stable'?'type':(infoData.stable_name?'value':'type')"
                :prop="infoType=='stable'?'value':(infoData.stable_name?'value':'type')"
                :width="infoType == 'stable' ? 100 : 150"
              >
              </el-table-column>
            </el-table>
          </el-form-item>
        </section>
        <section class="right">
          <el-form-item
            v-for="item in rightField"
            :key="item"
            :label="item + ':'"
          >
            {{ infoData[item] }}
          </el-form-item>
          <el-form-item v-if="infoType == 'stable'" label="columns:">
            <el-table
              tooltip-effect="light"
              style="width: 80%"
              size="mini"
              :data="columns"
            >
              <el-table-column
                :show-overflow-tooltip="true"
                min-width="100"
                label="name"
                prop="name"
              >
              </el-table-column>
              <el-table-column
                :show-overflow-tooltip="true"
                width="100"
                label="type"
                prop="value"
              >
              </el-table-column>
            </el-table>
          </el-form-item>
        </section>
      </section>
    </el-form>
  </div>
</template>

<script>
import { getStableStructReq } from "@/api/gateway/data/stables";
import { getTagValue, getMatrixStructReq } from "@/api/gateway/data/tables";
const customKey = ["noOperate", "parent", "node-key", "typeName"];
export default {
  data() {
    this.displayMap = {
      stable: ["name", "create_time"],
      table: ["table_name", "create_time", "stable_name", "columns"],
    };
    return {
      columns: [],
      tags: [],
    };
  },
  computed: {
    infoType() {
      return this.$store.state.console.currentInfoType;
    },
    infoData() {
      return this.$store.state.console.currentInfoData;
    },
    infoField() {
      return this.infoType == "database"
        ? Object.keys(this.infoData).filter((item) => {
            return !customKey.includes(item);
          })
        : this.displayMap[this.infoType];
    },
    leftField() {
      return this.infoField.filter((_, index) => index % 2 == 0);
    },
    rightField() {
      return this.infoField.filter((_, index) => index % 2);
    },
  },
  watch: {
    "infoData.name"() {
      this.getStruct();
    },
  },
  created() {
    this.getStruct();
  },
  methods: {
    getStruct() {
      switch (this.infoType) {
        case "stable":
          this.getStableStruct();
          break;
        case "table":
          this.getTableStruct();
          break;
        default:
          break;
      }
    },
    async getStableStruct() {
      // 当为超级表的时候只需要获取结构的类型就可以了
      let data = await getStableStructReq({
        selected_db: this.infoData.parent,
        stableName: this.infoData.name,
      }).catch(() => ({
        ts_field_name: "",
        columns: [],
        tags: [],
      }));
      this.columns = [{ name: data.ts_field_name, value: "timestamp" }].concat(
        data.columns.map((item) => ({ name: item.field, value: item.type }))
      );
      this.tags = data.tags.map((item) => ({
        name: item.field,
        value: item.type,
      }));
    },
    //普通表没有tag
    async getTableStruct() {
      let result = (await getMatrixStructReq({
        selected_db: this.infoData.parent.split(".")[0],
        selected_tb: this.infoData.name,
      }));
      let tags=[]
      if(this.infoData.stable_name){
        tags=result.filter((item) => item.typeName == "tag")
      }else{
        tags=result
      }
      let data = await getTagValue(
        tags.map((item) => ({ field: item.name })) || [],
        ...this.infoData.parent.split("."),
        this.infoData.name
      ).catch(() => []);
      this.tags = tags.map((item) => {
        item.value = data[0]?data[0][item.name]:item['dataType'];
        return item;
      });

    },
  },
};
</script>

<style lang="scss" scoped>
.info {
  height: 100%;
  .info-content {
    display: flex;
    justify-content: space-between;
    .left,
    .right {
      width: 48%;
    }
  }
}
.info ::v-deep .el-form-item__label {
  line-height: 20px !important;
  font-size: 16px;
}

.info ::v-deep .el-form-item__content {
  font-size: 16px;
  line-height: 22px;
}
.info ::v-deep .el-table {
  margin-top: -6px;
  & th.el-table__cell > .cell {
    padding-left: 0;
    font-size: 16px;
    font-weight: 500;
  }
  & td.el-table__cell > .cell {
    padding-left: 0;
    @extend .nowrap;
    font-size: 16px;
  }
}
</style>
