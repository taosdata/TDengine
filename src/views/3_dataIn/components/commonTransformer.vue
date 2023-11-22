<template>
  <div class="common-transformer">
    <!-- <section class="payload-upload">
      <el-form :model="ruleForm" label-width="200px">
        <el-form-item
          :label="$t('datasource.transformer.payload')"
          size="small"
        >
          <el-select v-model="ruleForm.payload">
            <el-option
              v-for="item in payloads"
              :key="item"
              :label="item"
              :value="item"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('datasource.transformer.upload')">
          <el-upload
            class="upload-demo"
            ref="upload"
            :data="uploadData"
            :action="uploadUrl"
            :on-success="handleSuccess"
            :file-list="fileList"
            :auto-upload="true"
          >
            <el-button slot="trigger" size="small" type="primary">{{
              $t("datasource.selectfile")
            }}</el-button>
          </el-upload>
        </el-form-item>
      </el-form>
    </section> -->
    <template v-if="columnsArr.length > 0">
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.identified") }}</span>
        </div>
        <ul class="col-list">
          <li v-for="(item, index) in columnsArr" :key="index">
            <span>{{ item.name }}</span>
          </li>
        </ul>
      </section>
      <section class="extract">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.extract") }}</span>
        </div>
        <template v-for="(item, index) in extractArr">
          <ExtractSplit
            :key="index"
            :data="item"
            :index="index"
            :extractColumns="columnsArr"
            @deleteExtract="deleteExtract"
          ></ExtractSplit>
        </template>

        <el-button type="primary" size="small" @click="addNewExtract"
          >Add</el-button
        >
      </section>
      <section class="filter">
        <div class="block-title">
          <span>{{ $t("datasource.transformer.filter") }}</span>
        </div>
        <template v-for="(item, index) in filterArr">
          <FilterExpression
            :key="index"
            :index="index"
            @deleteFilter="deleteFilter"
          ></FilterExpression>
        </template>
        <el-button type="primary" size="small" @click="addNewFilter"
          >Add</el-button
        >
      </section>
      <section>
        <div class="block-title">
          <span>{{ $t("datasource.transformer.superconfig") }}</span>
        </div>
        <div class="table-content">
          <div class="table-title">
            <div class="title">
              <span style="color: #4259ce">Target Super Table:</span>
              <el-form :model="sruleForm">
                <el-form-item prop="s_name">
                  <el-select
                    v-model="sruleForm.s_name"
                    filterable
                    allow-create
                    default-first-option
                    size="small"
                    @change="getSTbaleList"
                  >
                    <el-option
                      v-for="(item, index) in stableLists"
                      :key="index"
                      :label="item"
                      :value="item"
                    ></el-option>
                  </el-select>
                </el-form-item>
              </el-form>
            </div>
            <el-button type="primary" size="small" @click="createStable"
              >Create STable</el-button
            >
          </div>
          <div class="table-detail" v-if="tableData.length > 0">
            <el-table :data="tableData" border style="width: 100%">
              <template v-for="(item, index) in st_columnLists">
                <el-table-column
                  v-if="item === 'Expression'"
                  :key="index"
                  :prop="item"
                  show-overflow-tooltip
                  :label="item"
                >
                  <template slot-scope="scope">
                    <el-input v-model="scope.row.Expression" size="small"></el-input>
                  </template>
                </el-table-column>
                <el-table-column
                  v-else
                  :key="index"
                  :prop="item"
                  show-overflow-tooltip
                  :label="item"
                >
                </el-table-column>
              </template>
            </el-table>
          </div>
        </div>
      </section>
      <el-dialog
        :title="$t('datasource.transformer.create_st')"
        :visible.sync="showCreateDIalog"
        width="40%"
        center
        destroy-on-close
        @close="closeDialog"
      >
        <el-form :model="dialogForm" :rules="dialogRules">
          <el-form-item prop="st_name">
            <el-input v-model="dialogForm.st_name" size="small"></el-input>
          </el-form-item>
        </el-form>
        <div class="buttons">
          <el-button size="small" @click="closeDialog">{{
            $t("cancel")
          }}</el-button>
          <el-button type="primary" size="small" @click="createST">{{
            $t("ok")
          }}</el-button>
        </div>
      </el-dialog>
    </template>
  </div>
</template>
<script>
import ExtractSplit from "./extractSplit.vue";
import FilterExpression from "./filterExpression.vue";
import { getCSVColumns } from "@/api/explorer/datain";
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
export default {
  name: "CommonTransformer",
  components: { ExtractSplit, FilterExpression },
  props: {
    parserColumns: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      params_columns: [],
      params_tags: [],
      st_columnLists: [
        "Name",
        "Type",
        "Expression",
        "Sample Output1",
        "Sample Output2",
      ],
      dialogForm: {
        st_name: "",
      },
      dialogRules: {
        st_name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.st_input"),
          },
        ],
      },

      showCreateDIalog: false,
      stableLists: [],
      sruleForm: {
        s_name: "",
      },
      srules: {
        s_name: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.transformer.st_input"),
          },
        ],
      },
      uploadData: {
        req_id: new Date().getTime(),
      },
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      payloads: ["json", "csv"],
      fileList: [],
      ruleForm: {
        payload: "csv",
        file: "",
      },
      rules: [
        {
          payload: [
            {
              required: true,
              trigger: "blur",
            },
          ],
        },
      ],
      columnsArr: [],
      tableData: [],
      extractArr: [
        {
          columns: [],
          filters: [],
        },
      ],
      filterArr: [
        {
          expression: "",
        },
      ],
    };
  },
  mounted() {
    console.log(
      this.parserColumns,
      this.$store.state.app.currentDBName,
      "parserparserparser"
    );
    this.getInitStables();
  },
  methods: {
    closeDialog() {
      this.dialogForm.st_name = "";
      this.showCreateDIalog = false;
    },
    //创建或者查询
    async createST() {
      try {
        let result = await sendSQLReq(
          `CREATE STABLE if not exists \`${this.$store.state.app.currentDBName}\`.\`${this.dialogForm.st_name}\` (\`ts\` TIMESTAMP, \`i\` INT) TAGS (\`j\` INT)`
        );
        if (result.desc) {
          Message.error(result.desc);
          return;
        }
        Message.success(this.$t("operateSucc"));
        this.getInitStables();
        console.log(result, "创建超级表");
        this.closeDialog();
      } catch (error) {
        error.desc ? Message.error(error.desc) : "";
        console.log(error);
      }
    },
    //获取初始化的stables
    async getInitStables() {
      try {
        let result = await sendSQLReq(
          `show  \`${this.$store.state.app.currentDBName}\`.stables `
        );
        this.$set(this, "stableLists", Array.from(result.data).flat(1));
        console.log(result.data, this.stableLists, "初始化查询所有超级表");
      } catch (error) {
        console.log(error);
      }
    },
    createStable() {
      this.showCreateDIalog = true;
    },
    async getSTbaleList() {
      try {
        let res = await sendSQLReq(
          `desc \`${this.$store.state.app.currentDBName}\`.\`${this.sruleForm.s_name}\``
          //     {
          //   selected_db: this.$store.state.app.currentDBName,
          //   stableName: this.sruleForm.s_name,
          // }
        );
        if (res.desc) {
          Message.error(res.desc);
          return;
        }
        this.params_columns.splice(0, this.params_columns.length - 1);
        this.params_tags.splice(0, this.params_tags.length - 1);
        this.tableData = res.data.map((val, index) => {
          if (!val[3] && index > 0) {
            this.params_columns.push(val[0]); //存储非逐渐列
          }
          if (val.includes("TAG")) {
            this.params_tags.push(val[0]);
          }
          return {
            Name: val[0],
            Type: val[1],
            Expression: "",
            "Smaple Output1": "",
            "Smaple Output2": "",
          };
        });
        this.params_columns.unshift(res.data[0][0]);
        console.log(
          res,
          this.tableData,
          this.params_columns,
          this.params_tags,
          "获取超级表---999"
        );
      } catch (error) {
        console.log(error);
      }
    },
    handleSuccess(response, file, fileList) {
      console.log(response, file, fileList, "kkkkkkkkkkkkkk");
      this.getFileColumns(response[0], "csv", false);
    },
    async getFileColumns(path, type, hasHeader) {
      try {
        let result = await getCSVColumns(path, type, hasHeader);
        if (result?.file_header?.column_names) {
          this.$set(this, "columnsArr", result.file_header.column_names);
        }
        console.log(result, this.columnsArr, "查询结果");
      } catch (error) {
        console.log(error);
      }
    },
    //新增extract
    addNewExtract() {
      this.extractArr.push({
        columns: [],
        filters: [],
      });
    },
    //新增filter
    addNewFilter() {
      this.filterArr.push({
        expression: "",
      });
    },
    //删除filter
    deleteFilter(index) {
      this.filterArr.splice(index, 1);
    },
    deleteExtract(index) {
      this.extractArr.splice(index, 1);
    },
  },
  watch: {
    parserColumns: {
      deep: true,
      handler(val) {
        this.$set(this, "columnsArr", val);
        console.log(val, "000");
      },
    },
  },
};
</script>
<style lang="scss" scoped>
.block-title {
  margin-top: 25px;
  margin-bottom: 15px !important;
}
.extract {
  .el-button {
    width: 100%;
    margin-top: 20px;
  }
}
.col-list {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  column-gap: 15px;
  row-gap: 20px;
  max-height: 200px;
  overflow-y: auto;
  li {
    color: #4259ce;
    background: #ecf2fe;
    border-radius: 14px;
    border: 1px solid #f6f8fa;
    text-align: center;
  }
}
.filter {
  .el-button {
    width: 100%;
  }
  ::v-deep .el-input {
    margin-left: 0px !important;
  }
}
.table-title {
  display: flex;
  white-space: nowrap;
  align-items: center;
  .title {
    display: flex;
    align-items: center;
    flex: 1;
    .el-form-item {
      margin-bottom: 0px;
    }
    .el-select {
      width: 100%;
    }
    .el-form {
      flex: 1;
      margin-right: 15px;
      margin-left: 15px;
    }
  }
}
.table-detail {
  margin-top: 20px;
}
.payload-upload {
  .el-select {
    width: 100%;
  }
}
.upload-demo {
  display: flex;
  align-items: baseline;
}
.buttons {
  display: flex;
  justify-content: center;
  align-items: center;
  .el-button {
    width: 60px;
  }
}
</style>
