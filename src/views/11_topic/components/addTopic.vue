<template>
  <el-form
    class="add-topic"
    hide-required-asterisk
    ref="form"
    :rules="rules"
    style="text-align: left"
    size="small"
    label-width="120px"
    label-position="left"
    :model="info"
  >
    <p class="flexCenter">
      <el-radio-group size="small" v-model="model">
        <el-radio-button label="Wizard"></el-radio-button>
        <el-radio-button label="SQL"></el-radio-button>
      </el-radio-group>
    </p>
    <SQLEditor
      v-show="model == 'SQL'"
      :placeholder="sqlTip"
      ref="sqlStr"
      v-model="sqlStr"
    ></SQLEditor>
    <template v-if="model == 'Wizard'">
      <el-form-item :label="$t('topic.topicName')" prop="topic_name">
        <el-input v-model="info.topic_name" maxlength="32"> </el-input>
      </el-form-item>
      <!-- <SQuery
        ref="subquery"
        :dbList.sync="dbList"
        :fieldSet="fieldSet"
        :level="subqueryLevel"
        :info="info"
      >
        <template #db-bottom>
          <el-form-item :label="$t('type')" prop="topic_type" required>
            <el-radio-group size="small" v-model="info.topic_type">
              <el-radio-button label="DATABASE">{{
                $t("stream.databaseUpper")
              }}</el-radio-button>
              <el-radio-button label="STABLE">{{
                $t("stream.stableUpper")
              }}</el-radio-button>
              <el-radio-button label="SUBQUERY">{{
                $t("stream.subqueryUpper")
              }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item
            v-if="info.topic_type == 'SUBQUERY'"
            :label="$t('stream.tableType')"
            prop="table_type"
            required
          >
            <el-radio-group size="small" v-model="info.table_type">
              <el-radio-button label="STABLE">{{
                $t("stream.stableUpper")
              }}</el-radio-button>
              <el-radio-button label="TABLE">{{
                $t("stream.tableUpper")
              }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </SQuery> -->
       <Subquery ref="subquery" :dbList.sync="dbList" :fieldSet="fieldSet" :level="subqueryLevel" :info="info">
        <template #db-bottom>
          <el-form-item :label="$t('type')" prop="topic_type" required>
            <el-radio-group size="small" v-model="info.topic_type">
              <el-radio-button label="DATABASE">{{ $t("stream.databaseUpper") }}</el-radio-button>
              <el-radio-button label="STABLE">{{ $t("stream.stableUpper") }}</el-radio-button>
              <el-radio-button label="SUBQUERY">{{ $t("stream.subqueryUpper") }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="info.topic_type == 'SUBQUERY'" :label="$t('stream.tableType')" prop="table_type" required>
            <el-radio-group size="small" v-model="info.table_type">
              <el-radio-button label="STABLE">{{ $t("stream.stableUpper") }}</el-radio-button>
              <el-radio-button label="TABLE">{{ $t("stream.tableUpper") }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </Subquery>
      <!-- <el-form-item :label="$t('topic.conditionSet')"></el-form-item> -->
    </template>
    <p v-if="errorText" class="errorText">{{ errorText }}</p>
    <el-form-item v-if="model == 'Wizard'">
      <div class="flexBetween">
        <el-button
          style="width: 30%"
          :loading="requestIng"
          :disabled="createBtn"
          type="primary"
          @click="handleCreateTopic"
          >{{ $t("create") }}</el-button
        >
        <el-button :disabled="previewBtn" @click="generateSql">{{
          $t("sqlPreview")
        }}</el-button>
      </div>
    </el-form-item>
    <div v-else class="flexCenter">
      <el-button
        size="small"
        style="width: 30%"
        :disabled="createBtn"
        type="primary"
        @click="handleCreateTopic"
        >{{ $t("create") }}</el-button
      >
    </div>
    <el-dialog
      custom-class="show-topic-sql"
      width="500px"
      append-to-body
      :visible.sync="dialog"
      title="SQL"
    >
      <pre :key="previewSql" v-highlight>
        <code class="language-sql">{{previewSql}}</code>
      </pre>
      <section class="flexEnd">
        <el-button type="primary" size="mini" @click="dialog = false">{{
          $t("confirm")
        }}</el-button>
      </section>
    </el-dialog>
  </el-form>
</template>

<script>
import SQuery from "./subscribeQuery.vue";
import Subquery from "./subquery.vue";
import SQLEditor from "./sqlEditor.vue";
import { createTopic } from "@/api/topic";
import { validTopicSql, validDatabaseName } from "@/utils/validate"
// const infoValidaterField = ["topic_name", "topic_type", "db_name"];
export default {
  props: {
    topicList: {
      type: Array,
      default: () => [],
    },
  },
  components: { SQLEditor, SQuery,Subquery },
  data() {
    const validateTopicName = (_, val, callback) => {
      if (!val) {
        callback(new Error(this.$t("topic.topicNameError")));
      } else if (this.topicList.some((item) => item.topicName === val)) {
        callback(new Error(this.$t("topic.topicNameExist")));
      } else if (!validDatabaseName(val)) {
        callback(new Error(this.$t("formatWrong")))
      } else {
        callback();
      }
    };
    return {
      sqlPrefix: "CREATE TOPIC ",
      rules: {
        topic_name: [{ 
          validator: validateTopicName, 
          trigger: "blur", 
          required: true 
        }],
        stbName: [{
          required: true,
          message: this.$t("stream.stableUpperRequired") 
        }],
        tbName: [{
          required: true,
          message: this.$t("stream.tableUpperRequired") 
        }],
      },

      sqlStr: "",
      model: "Wizard",
      info: {
        db_name: "",
        stbName: "",
        tbName: "",
        resultSet: [],
        topic_type: "STABLE",
        topic_name: "",
        table_type: "STABLE",
      },
      stableList: [],
      tableList: [],
      dbList: [],
      errorText: "",
      requestIng: false,
      previewSql: "",
      dialog: false,
      sqlTip:
        "CREATE TOPIC [IF NOT EXISTS] topic_name AS {subquery | DATABASE db_name | STABLE stb_name }",
    };
  },
  computed: {
    params() {
      if (this.model !== "Wizard" || this.info.topic_type !== "SUBQUERY")
        return {};
      const result = {
        selected_db: this.info.db_name,
      };
      if (this.info.table_type === "STABLE") {
        result.stableName = this.info.stbName;
      } else {
        result.selected_tb = this.info.tbName;
      }
      return result;
    },
    previewBtn() {
      if (this.model === "Wizard") {
        if (!this.info.topic_name) return true;
        if (!this.info.db_name) return true;
        if (this.info.topic_type === "STABLE") {
          return !this.info.stbName;
        } else if (this.info.topic_type === "SUBQUERY") {
          return this.info.table_type == "STABLE"
            ? !this.info.stbName
            : !this.info.tbName;
        } else {
          return false;
        }
      } else {
        return true;
      }
    },
    createBtn() {
      return (
        this.requestIng ||
        (this.model === "Wizard" && this.previewBtn) ||
        (this.model === "SQL" && !this.sqlStr)
      );
    },
    subqueryLevel() {
      return {
        DATABASE: 0,
        STABLE: 1,
        SUBQUERY: {
          STABLE: 1,
          TABLE: 2,
        }[this.info.table_type],
      }[this.info.topic_type];
    },
    fieldSet() {
      return this.model === "Wizard" && this.info.topic_type === "SUBQUERY";
    },
  },
  watch: {},
  mounted() {},
  methods: {
    async handleCreateTopic() {
      this.errorText = "";
      if (this.requestIng) return;
      let params = {};
      if (this.model === "Wizard") {
        await this.generateSql(false);
        // params = {
        //   database_id: this.dbList.find(
        //     (item) => item.name === this.info.db_name
        //   ).databaseId,
        //   topic_sql: this.previewSql,
        //   topic_type: this.info.topic_type,
        //   topic_name: this.info.topic_name,
        //   db_name: this.info.db_name,
        // };
        params=this.previewSql
      } else {
        let sqlobj = this.handleSQLParams();
        if(validTopicSql(sqlobj.topic_sql.trimStart())) {
          params = sqlobj.topic_sql
        } else {
          this.errorText = this.$t('topic.validTopicSqlDesc');
          return
        }
      }
      this.requestIng = true;
      createTopic(params)
        .then(() => {
          this.$refs.form.resetFields();
          this.info.stbName = "";
          this.info.tbName = "";
          this.sqlStr = "";
          this.$message.success(this.$t("addSucc"));
          this.$emit("close");
        })
        .catch((err) => (this.errorText = err?.desc))
        .finally(() => {
          this.requestIng = false;
        });
    },
    handleSQLParams() {
      let database = "";
      let topic_type = "";
      let topic_name = this.sqlStr.match(/topic\s+(\w+)/i)?.[1];
      if (/database/i.test(this.sqlStr)) {
        database = this.sqlStr.match(/database\s+`*(\w+)/i)?.[1];
        topic_type = "DATABASE";
      } else if (/stable/i.test(this.sqlStr)) {
        database = this.sqlStr.match(/stable\s+`*(\w+)/i)?.[1];
        topic_type = "STABLE";
      } else {
        database = this.sqlStr.match(/from\s+`*(\w+)/i)?.[1];
        topic_type = "SUBQUERY";
      }
      let database_id = database
        ? this?.dbList.find((item) => item.name === database)?.name
        : "";
        
      return {
        database_id,
        topic_type,
        topic_name,
        topic_sql: this.sqlStr,
        db_name: database,
      };
    },
    generateSql(show = true) {
      return new Promise((resolve, reject) => {
        this.$refs.form.validate((valid) => {
          if (valid) {
            const dbname = this.info.db_name.toLowerCase();
            if (this.info.topic_type == "DATABASE") {
              this.previewSql =
                this.sqlPrefix +
               "`"+this.info.topic_name +"`"+
                "  WITH META AS DATABASE `" +
                dbname +
                "`";
            } else if (this.info.topic_type == "STABLE") {
              this.previewSql =
                this.sqlPrefix +
                "`"+this.info.topic_name +"`"+
                ` with meta AS STABLE \`${dbname}\`.\`${this.info.stbName}\``;
            } else {
              const subquery = this.$refs.subquery.getResultSet() || "";
              this.previewSql =
                this.sqlPrefix + "`"+this.info.topic_name+"`" + " AS " + subquery;
            }
            if (show) this.dialog = true;
            resolve(this.previewSql);
            // const h = this.$createElement;
            // show &&
            //   this.$alert(
            //     h(
            //       "pre",
            //       {
            //         class: "pre-code",
            //         directives: [
            //           {
            //             name: "highlight",
            //           },
            //         ],
            //         key: Date.now(),
            //       },
            //       [h("code", { class: "language-sql" }, this.previewSql)]
            //     ),
            //     "SQL",
            //     {
            //       customClass: "show-topic-sql",
            //     }
            //   );
          } else {
            reject();
          }
        });
      });
    },
  },
};
</script>

<style scoped lang="scss">
.add-topic {
  .flexCenter {
    margin-bottom: 20px;
  }
  .vue-codemirror {
    height: 100px;
    margin-bottom: 20px;
  }
  &:deep(.CodeMirror) {
    height: 100px;
    .CodeMirror-placeholder {
      color: #c0c4cc;
    }
  }
}
.language-sql {
  white-space: normal;
  word-break: break-all;
  word-wrap: break-word;
}
</style>
<style>
.show-topic-sql .pre-code {
  text-align: left;
  background-color: #f6f8fa;
  padding: 5px;
  white-space: break-spaces;
}
</style>
