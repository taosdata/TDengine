<template>
  <div class="info">
    <!-- 数据库 超级表 字表 表  -->
    <el-form label-width="180px" :action="'#'" label-position="right" v-if="infoType == 'database'">
      <section class="info-content">
        <section class="left">
          <el-form-item v-for="item in leftField" :key="item" :label="item + ':'">
            {{ infoData[item] }}
          </el-form-item>
        </section>
        <section class="right">
          <el-form-item v-for="item in rightField" :key="item" :label="item + ':'">
            {{ infoData[item] }}
          </el-form-item>
        </section>
        <section class="dsn" v-if="infoType === 'database'">
          <el-form-item :key="`dsn-for-${infoData['name']}`" :label="'DSN:'">
            <div class="copy-wrapper">
              <!-- <div class="nowrap">{{ dsn + "/" + infoData["name"] }}</div>  -->
              <el-tooltip
                placement="top"
                :content="`${dsn}/${infoData['name']}`"
                effect="light"
              >
                <el-button class="copy-btn" type="text" size="mini" @click="copyDsn">
                  <el-icon class="el-icon-copy-document"></el-icon>
                  {{ $t("copy") }}
                </el-button>
              </el-tooltip>
            </div>
          </el-form-item>
        </section>
      </section>
    </el-form>
    <section v-else>
      <el-descriptions size=“mini” :column="2">
        <el-descriptions-item v-for="item in infoField" :label="$t(`console.${item}`)" :key="item">{{ infoData[item] }}</el-descriptions-item>
      </el-descriptions>
      <el-table tooltip-effect="light" size="mini" :data="tableData" border>
        <el-table-column :show-overflow-tooltip="true" :label="$t('console.category')" prop="category" width="90px">
        </el-table-column>
        <el-table-column :show-overflow-tooltip="true" :label="$t('console.name')" prop="name" min-width="200">
        </el-table-column>
        <el-table-column :show-overflow-tooltip="true" :label="$t('console.type')" prop="type" min-width="100">
        </el-table-column>
        <el-table-column :show-overflow-tooltip="true" :label="$t('console.encode')" prop="encode" width="90px" v-if="version_gt_3300">
        </el-table-column>
        <el-table-column :show-overflow-tooltip="true" :label="$t('console.compress')" prop="compress" width="180px" v-if="version_gt_3300">
        </el-table-column>
        <el-table-column :show-overflow-tooltip="true" :label="$t('console.level')" prop="level" width="150px" v-if="version_gt_3300">
        </el-table-column>
      </el-table>
    </section>
  </div>
</template>

<script>
import Prism from "prismjs";
import "prismjs/themes/prism.css";
import "prismjs/components/prism-bash";
import { copy } from "@/utils/index";
import { getStableStructReq } from "@/api/gateway/data/stables";
import { getTagValue, getMatrixStructReq } from "@/api/gateway/data/tables";
import { getDSN } from "@/utils/index";
import VersionMixin from "@/mixins/version";
const customKey = ["noOperate", "parent", "node-key", "typeName"];
export default {
  data() {
    this.displayMap = {
      stable: ["stable_name", "create_time", "columns", "tags"],
      CHILD_TABLE: ["table_name", "create_time", "stable_name", "columns", "tags"],
      NORMAL_TABLE: ["table_name", "create_time", "columns"],
    };
    return {
      columns: [],
      tags: [],
      tableData: []
    };
  },
  mixins: [VersionMixin],
  computed: {
    infoType() {
      return this.$store.state.console.currentInfoType;
    },
    infoData() {
      return this.$store.state.console.currentInfoData;
    },
    dsn() {
      return getDSN("taos");
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
  mounted() {
    Prism.highlightAll();
  },
  methods: {
    copyDsn() {
      copy(this.dsn +"/" + this.infoData["name"]);
    },
    getStruct() {
      switch (this.infoType) {
        case "stable":
          this.getStableStruct();
          break;
        case "CHILD_TABLE":
        case "NORMAL_TABLE":
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

      const { encode, compress, level } = data;
      this.columns = [{ name: data.ts_field_name, type: "timestamp", category: 'Column', encode, compress, level }].concat(
        data.columns.map((item) => ({ 
          ...item, 
          name: item.field, 
          type: item.note ? item.type + '(' + item.note + ')' : item.type, 
          category: 'Column',
        }))
      );

      this.tags = data.tags.map((item) => ({
        ...item,
        name: item.field,
        type: item.type,
        category: 'Tag'
      }));

      this.tableData = this.columns.concat(this.tags)
    },
    //普通表没有tag
    async getTableStruct() {
      let result = (await getMatrixStructReq({
        selected_db: this.infoData.parent.split(".")[0],
        selected_tb: this.infoData.name,
      }));
      
      if (this.infoType == 'CHILD_TABLE') {
        this.infoData.tags = result.filter((item) => item.typeName == "tag").length
      }

      let data = await getTagValue(
        result.filter((item) => item.typeName == "tag").map((item) => ({ field: item.name })) || [],
        ...this.infoData.parent.split("."),
        this.infoData.name
      ).catch(() => []);

      this.tableData = result.map((item) => {
        item.value = data[0] ? data[0][item.name] : item['dataType'];
        item.name = item.typeName == 'tag' ? item.name + '(' + item.value + ')' : item.name
        item.type = item.note ? item.type + '(' + item.note + ')' : item.type
        item.category = item.typeName == 'tag' ? 'Tag' : 'Column'
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
    flex-wrap: wrap;

    section.left,
    section.right {
      width: 48%;
    }

    section.dsn {
      width: 100%;
    }
  }
}
.info ::v-deep .el-form-item {
  margin-bottom: 8px !important;
}
.info ::v-deep .el-form-item__label {
  line-height: 20px !important;
  font-size: 16px;
  text-align: right;
}

.info ::v-deep .el-form-item__content {
  font-size: 16px;
  line-height: 22px;
  color: rgb(144, 147, 153);
}

.info ::v-deep .el-table--border .el-table__cell {
  border-right: none !important;
}
.info ::v-deep .el-descriptions {
  padding: 0 10px;
}
.info ::v-deep .el-descriptions-item__container {
  align-items: center;
}
.info ::v-deep .el-descriptions-item__content {
  font-size: 16px;
  color: #4d6992;
}
.dsn ::v-deep .el-form-item {
  display: flex;
  align-items: center;
}
.dsn ::v-deep .el-form-item__content {
 margin-left: 0 !important;
}
.copy-wrapper {
  display: flex;
  flex-wrap: nowrap;
  justify-content: space-between;
  >div {
   max-width: 700px;
  }
}
.copy-btn {
  font-size: 12px;
  cursor: pointer;
}
</style>
