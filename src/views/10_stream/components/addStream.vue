<template>
  <el-form
    class="add-topic"
    ref="form"
    hide-required-asterisk
    :rules="rules"
    style="text-align: left"
    size="small"
    label-width="150px"
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
      <el-form-item
        :label="$t('stream.streamName')"
        required
        prop="stream_name"
      >
        <el-input v-model="info.stream_name"> </el-input>
      </el-form-item>
      <h1 class="part-title">{{ $t("stream.output") }}</h1>
      <el-form-item :label="$t('stream.database')" required prop="target_db">
        <el-select
          class="w100"
          v-model="info.target_db"
          @change="info.target_stb = ''"
          placeholder=""
        >
          <el-option
            v-for="item in dbList"
            :key="item.name"
            :value="item.name"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.stable')" required prop="target_stb">
        <el-input :disabled="!info.target_db" v-model="info.target_stb">
        </el-input>
      </el-form-item>
      <el-form-item :label="$t('stream.subtablePrefix')" prop="subtale">
        <el-input
          :disabled="!info.target_stb"
          v-model="info.subtale"
          placeholder=""
        >
        </el-input>
      </el-form-item>
      <h1 class="part-title">{{ $t("stream.source") }}</h1>
      <Subquery
        :level="info.source_type"
        :avgFn="true"
        ref="subquery"
        :windowClause="true"
        :dbList.sync="dbList"
        fieldSet
        :parttion="true"
        :info="info"
      >
        <template #db-bottom>
          <el-form-item :label="$t('type')" prop="source_type" required>
            <el-radio-group size="small" v-model="info.source_type">
              <el-radio-button :label="1">{{
                $t("stream.stableUpper")
              }}</el-radio-button>
              <el-radio-button :label="2">{{
                $t("stream.tableUpper")
              }}</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </Subquery>
      <h1 class="part-title">{{ $t("stream.execution") }}</h1>
      <el-form-item :label="$t('stream.trigger')">
        <el-select class="w100" v-model="info.trigger" placeholder="">
          <el-option
            v-for="item in triggerList"
            :key="item.value"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item
        v-if="info.trigger == 'MAX_DELAY'"
        :label="$t('stream.maxDelayTime')"
      >
        <el-input-number
          :min="0"
          v-model="info.max_delay_time"
        ></el-input-number>
        <el-select
          style="margin-left: 20px"
          v-model="info.max_delay_unit"
          placeholder=""
        >
          <el-option
            v-for="item in watermarkUnitList"
            :key="item.label"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.delay')">
        <template slot="label">
          <span>{{ $t("stream.delay") }}&nbsp;</span>
          <el-tooltip
            effect="light"
            :content="$t('stream.delaytip')"
            placement="top"
          >
            <i class="el-icon-info"></i>
          </el-tooltip>
        </template>
        <el-input-number
          :min="0"
          :max="watermarkMax"
          v-model="info.watermark"
        ></el-input-number>
        <el-select
          style="margin-left: 20px"
          v-model="info.watermark_unit"
          @change="watermarkUnitChange"
          placeholder=""
        >
          <el-option
            v-for="item in watermarkUnitList"
            :key="item.label"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Ignore Expired">
        <template slot="label">
          <span>Ignore Expired</span>
        </template>
        <el-select
          v-model="info.ignore_expired"
          @change="changeIgnoreExpired"
          placeholder=""
        >
          <el-option
            v-for="item in expiredList"
            :key="item.label"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
    </template>
    <p v-if="errorText" class="errorText">{{ errorText }}</p>
    <el-form-item v-if="model == 'Wizard'">
      <div class="flexBetween">
        <el-button
          style="width: 30%"
          :disabled="createBtn"
          type="primary"
          @click="handlecreateStream"
          >{{ $t("create") }}</el-button
        >
        <el-button :disabled="previewBtn" @click="generateSql"
          >{{ $t('sqlPreview') }}</el-button
        >
      </div>
    </el-form-item>
    <div v-else class="flexCenter">
      <el-button
        size="small"
        style="width: 30%"
        :loading="requestIng"
        :disabled="createBtn"
        type="primary"
        @click="handlecreateStream"
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
import SQLEditor from "@/views/11_topic/components/sqlEditor.vue";
import { createStream } from "@/api/stream";
import { isStableExist } from "@/api/gateway/data/stables";
import Subquery from "@/views/11_topic/components/subquery.vue";
import { validStreamSql } from "@/utils/validate"
// const infoValidaterField = ["stream_name", "topic_type", "db_name"];
export default {
  props: {
    streamList: {
      type: Array,
      default: () => [],
    },
  },
  components: { SQLEditor, Subquery },
  data() {
    const validateTopicName = (_, val, callback) => {
      if (!val) {
        callback(new Error(this.$t("stream.streamNameError")));
      } else if (this.streamList.some((item) => item.stream_name === val)) {
        callback(new Error(this.$t("stream.streamNameExist")));
      } else {
        callback();
      }
    };
    const validateTargetStb = async (_, val, callback) => {
      // if (await isStableExist(val, this.info.target_db)) {
      //   callback(new Error(this.$t("stream.stableExist")));
      // } else {
      callback();
      // }
    };
    return {
      sqlPrefix: "CREATE STREAM ",
      rules: {
        stream_name: [{ validator: validateTopicName }],
        target_stb: [{ 
          // validator: validateTargetStb, 
          required: true,
          message: this.$t("stream.stableUpperRequired") 
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
      cmOptions: {
        tabSize: 2,
        mode: "text/x-sql",
        theme: "eclipse",
        lineNumbers: true,
        spellcheck: true, //拼写检查
        line: true,
        hintOptions: { completeSingle: false },
        viewportMargin: 2,
        autofocus: false,
        showCursorWhenSelecting: true,
        extraKeys: {
          Tab: "autocomplete",
          "Shift-Enter": () => this.handleSendSQL(),
          "Shift-Return": () => this.handleSendSQL(),
        },
        gutters: [
          "CodeMirror-lint-markers", //代码错误检测
          "CodeMirror-linenumbers",
          "CodeMirror-foldgutter", //展开收起
        ],
        autocompletion: {},
      },
      sqlStr: "",
      model: "Wizard",
      dbList: [],
      info: {
        db_name: "",
        target_db: "",
        target_stb: "",
        stbName: "",
        tbName: "",
        resultSet: [],
        source_type: 1,
        subtale: "",
        stream_name: "",
        parttionSet: "tbname",
        window_type: "INTERVAL",
        table_type: "STABLE",
        tol_val: "",
        tol_unit: "m",
        interval_val: "1",
        state_column: "",
        interval_unit: "m",
        sliding_val: "",
        sliding_unit: "s",
        trigger: "WINDOW_CLOSE",
        max_delay_time: "",
        max_delay_unit: "s",
        watermark: 0,
        watermark_unit: "s",
        ignore_expired: 1,
      },
      watermarkMax: 15 * 60,
      expiredList: [
        {
          label: 1,
          value: 1,
        },
        {
          label: 0,
          value: 0,
        },
      ],
      watermarkUnitList: [
        {
          label: "second",
          value: "s",
        },
        {
          label: "minute",
          value: "m",
        },
      ],
      triggerList: [
        {
          label: "WINDOW_CLOSE",
          value: "WINDOW_CLOSE",
        },
        {
          label: "AT_ONCE",
          value: "AT_ONCE",
        },

        {
          label: "MAX_DELAY",
          value: "MAX_DELAY",
        },
      ],
      stableList: [],
      tableList: [],
      errorText: "",
      requestIng: false,
      previewSql: "",
      dialog: false,
      sqlTip:
        "CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery",
    };
  },
  computed: {
    targetDBConfig() {
      return {
        label: this.$t("stream.targetDB"),
        field: "targetDB",
      };
    },
    targetStbConfig() {
      return {
        label: this.$t("stream.targetStable"),
        field: "targetStb",
      };
    },
    previewBtn() {
      if (this.model === "Wizard") {
        if (
          !this.info.stream_name ||
          !this.info.target_db ||
          !this.info.db_name
        )
          return true;
        if (this.info.source_type === 1) {
          return !this.info.stbName;
        } else if (this.info.source_type === 2) {
          return !this.info.tbName;
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
  },
  methods: {
    changeIgnoreExpired() {},
    async handlecreateStream() {
      this.errorText = "";
      if (this.requestIng) return;
      let sql = "";
      if (this.model === "Wizard") {
        sql = await this.generateSql(false);
      } else {
        if(validStreamSql(this.sqlStr.trimStart())) {
          sql = this.sqlStr;
        } else {
          this.errorText = this.$t('stream.validStreamSqlDesc');
          return
        }
      }
      this.requestIng = true;
      createStream(sql)
        .then(() => {
          this.$refs.form.resetFields();
          this.sqlStr = "";
          this.$message.success(this.$t("addSucc"));
          this.$emit("close");
        })
        .catch((err) => (this.errorText = err?.desc))
        .finally(() => {
          this.requestIng = false;
        });
    },
    watermarkUnitChange(val) {
      if (val === "s") {
        this.watermarkMax = 15 * 60;
      } else {
        if (this.info.watermark > 15) {
          this.info.watermark = 15;
        }
        this.watermarkMax = 15;
      }
    },
    generateSql(show = true) {
      return new Promise((resolve, reject) => {
        this.$refs.form.validate((valid) => {
          if (valid) {
            try {
              const subquery = this.$refs.subquery.getResultSet() || "";
              let previewSql =
                this.sqlPrefix +
                "`" +
                this.info.stream_name +
                "`" +
                " TRIGGER " +
                this.info.trigger +
                " ";

              previewSql += `  IGNORE EXPIRED ${this.info.ignore_expired} `;

              if (this.info.trigger === "MAX_DELAY") {
                previewSql +=
                  this.info.max_delay_time + this.info.max_delay_unit;
              }
              if (this.info.watermark) {
                previewSql +=
                  " WATERMARK " +
                  this.info.watermark +
                  this.info.watermark_unit;
              }

              previewSql +=
                " INTO `" +
                this.info.target_db.toLowerCase() +
                "`.`" +
                this.info.target_stb +
                "`";
              if (this.info.subtale) {
                previewSql += ` SUBTABLE(CONCAT('${this.info.subtale}',tbname))`;
              }
              previewSql += " AS " + subquery;
              if (this.info.parttionSet && this.info.parttionSet.length > 0) {
                previewSql += " PARTITION BY " + this.info.parttionSet;
              }
              if (this.info.window_type) {
                previewSql += " ";
                const ts_col = this.info.resultSet.find(
                  (item) => item.type === "TIMESTAMP"
                )?.field;
                switch (this.info.window_type) {
                  case "SESSION":
                    previewSql += `SESSION(${ts_col},${this.info.tol_val}${this.info.tol_unit})`;
                    break;
                  case "STATE":
                    previewSql += `STATE_WINDOW(\`${this.info.state_column}\`)`;
                    break;
                  case "INTERVAL":
                    previewSql += `INTERVAL(${this.info.interval_val}${this.info.interval_unit})`;
                    if (this.info.sliding_val) {
                      previewSql += ` SLIDING(${this.info.sliding_val}${this.info.sliding_unit})`;
                    }
                    break;
                  default:
                    break;
                }
              }
              this.previewSql = previewSql;
              if (show) this.dialog = true;
              resolve(previewSql);
            } catch (error) {
              console.log(error);
              reject(error);
            }
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
.source-content {
  padding: 10px;
}
.part-title {
  font-size: 18px;
  line-height: 36px;
  color: #4d6992;
  font-weight: bold;
  text-align: center;
}
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
  }
}
:deep {
  .el-input-number__increase,
  .el-input-number__decrease {
    height: 30px;
    display: flex;
    justify-content: center;
    align-items: center;
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

.el-input-number__increase,
.el-input-number__decrease {
  height: 26px;
  display: flex;
  justify-content: center;
  align-items: center;
}

</style>
