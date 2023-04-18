<template>
  <div>
    <el-table tooltip-effect="dark" style="width: 100%" :data="value" size="mini">
      <el-table-column width="40">
        <template slot-scope="scope">
          <el-checkbox v-model="scope.row.checked"></el-checkbox>
        </template>
      </el-table-column>
      <el-table-column :label="$t('name')" show-overflow-tooltip prop="name" min-width="120"> </el-table-column>
      <el-table-column prop="type" :label="$t('type')" show-overflow-tooltip width="120"> </el-table-column>
      <el-table-column prop="result" :label="$t('topic.resultSet')" width="120">
        <template slot-scope="{ row, $index }">
          <el-button :disabled="!row.fnList" @click="result(row, $index)" icon="el-icon-setting" size="mini"></el-button>
        </template>
      </el-table-column>
      <el-table-column prop="condition" :label="$t('topic.conditionSet')" width="120">
        <template slot-scope="{ row, $index }">
          <el-button @click="result(row, $index, 1)" :disabled="!row.conditionList.length" icon="el-icon-setting" size="mini"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog append-to-body width="400px" center :title="title" :visible.sync="dialog">
      <component :is="comp" v-bind="dialogParams"></component>
      <section slot="footer">
        <el-button @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button @click="confirm" type="primary">{{ $t("confirm") }}</el-button>
      </section>
    </el-dialog>
  </div>
  <!-- <ul class="result-set">
    <el-empty v-if="!options.length" :image-size="100"></el-empty>
    <li v-for="item in options" :key="item.field">
      <el-checkbox v-model="item.checked"></el-checkbox>
      <p :title="item.filed + ' (' + item.type + ')'" class="no-wrap">{{ item.filed }} ({{ item.type }})</p>
      <el-select v-if="item.fnList" size="small" :placeholder="defaultFnName" v-model="item.fn">
        <el-option v-for="ite in item.fnList" :key="ite" :value="ite"></el-option>
      </el-select>
    </li>
  </ul> -->
</template>

<script>
  import Result from "./result.vue";
  import Condition from "./condition.vue";
  import { deepClone } from "@/utils";
  import { getMatrixStructReq } from "@/api/gateway/data/tables";
  import { getStableStructReq } from "@/api/gateway/data/stables";
  import {
    NumbericFn,
    StringFn,
    TDengineStringType,
    TDengineNumberType,
    CompareOperator,
    JsonOperator,
    GeneralOperator,
    AggregationFn,
  } from "@/const";
  import { isArray } from "@/utils/validate";
  const fnMap = {
    NUMBER: NumbericFn,
    STRING: StringFn,
    AVGFN: AggregationFn,
  };
  const getGeneralFn = type => {
    return GeneralOperator.filter(item => !item.include || !item.include.includes(type)).map(item => item.label);
  };
  const conditionMap = {
    TIMESTAMP: CompareOperator.concat(getGeneralFn("TIMESTAMP")),
    NUMBER: CompareOperator.concat(getGeneralFn("NUMBER")),
    STRING: getGeneralFn("STRING"),
    JSON: JsonOperator.concat(getGeneralFn("JSON")),
    BOOL: ["=="].concat(getGeneralFn("BOOL")),
  };
  export default {
    model: {
      prop: "value",
      event: "change",
    },
    props: {
      params: {
        type: Object,
        default: () => ({}),
      },
      value: {
        type: Array,
        default: () => [],
      },
      avgFn: {
        type: Boolean,
        default: false,
      },
    },
    components: { Result, Condition },
    data() {
      return {
        options: [],
        fnList: [],
        defaultFnName: "Function",
        dialog: false,
        dialogType: 0,
        dialogParams: {},
        currentRowIndex: -1,
        tags: [],
      };
    },
    computed: {
      comp() {
        return {
          0: Result,
          1: Condition,
        }[this.dialogType];
      },
      title() {
        return {
          0: this.$t("topic.resultSet"),
          1: this.$t("topic.conditionSet"),
        }[this.dialogType];
      },
    },
    watch: {
      params: {
        handler() {
          this.getData();
        },
        deep: true,
        immediate: true,
      },
    },
    created() {},
    mounted() {},
    methods: {
      getData() {
        if (!this.params.selected_db || (!this.params.selected_tb && !this.params.stableName)) return this.$emit("change", []);
        const dataFn = this.params.stableName ? getStableStructReq : getMatrixStructReq;
        dataFn(this.params)
          .then(data => {
            let fields = [];
            this.$emit("update:columns", data.columns || data);
            if (!isArray(data)) {
              data.columns.push({ type: "TIMESTAMP", field: data.ts_field_name });
              fields = data.columns.concat(data.tags || []);
            } else {
              fields = data;
            }
            this.$emit("update:tags", data.tags || []);
            const result = [];
            if (this.avgFn) {
              result.push({
                field: "*",
                name: "*",
                fieldList: fields,
                result: this.handleFnParamsFiled(fnMap.AVGFN),
                fnList: fnMap.AVGFN,
                checked: true,
                conditionList: [],
                condition: [],
              });
            }
            try {
              fields.forEach(item => {
                const fnList = fnMap[this.getType(item.type, "result")];
                result.push({
                  type: item.type,
                  field: `\`${item.field}\``,
                  name: item.field,
                  fieldList: fields,
                  result: this.handleFnParamsFiled(fnList),
                  condition: [
                    {
                      value: "",
                      key: 1,
                      operator: "",
                    },
                  ],
                  checked: true,
                  fnList,
                  conditionList: conditionMap[this.getType(item.type)],
                });
              });
            } catch (error) {
              console.log(error);
            }
            console.log(result,'结果集0000');
            this.$emit("change", result);
          })
          .catch(() => (this.options = []));
      },
      handleFnParamsFiled(fnList = []) {
        const result = {
          fn: "",
        };
        fnList.forEach(item => {
          if (item.filters) {
            result.params = {};
            item.filters.forEach(it => {
              result.params[it.field] = it.defaultValue;
            });
          }
        });
        return result;
      },
      handleDisabled(row) {
        return !row.conditionList.length;
      },
      getType(type, fnType) {
        if (this.avgFn && fnType == "result") return "AVGFN";
        if (!type) return "";
        type = type.replace(/\(\d+\)/, "");
        if (TDengineStringType.includes(type)) return "STRING";
        if (TDengineNumberType.includes(type)) return "NUMBER";
        return type;
      },
      result(row, index, type = 0) {
        this.dialog = true;
        this.currentRowIndex = index;
        this.dialogType = type;
        this.dialogParams = deepClone(row);
      },

      confirm() {
        switch (this.dialogType) {
          case 0:
            this.value[this.currentRowIndex].result = this.dialogParams.result;
            break;
          case 1:
            this.value[this.currentRowIndex].condition = this.dialogParams.condition;
            break;
          default:
            break;
        }
        this.dialog = false;
      },
    },
  };
</script>

<style scoped lang="scss">
  .result-set {
    max-height: 300px;
    li {
      display: grid;
      align-items: center;
      grid-template-columns: 16px auto 100px;
      grid-gap: 10px;
      padding: 5px;
    }
  }
</style>
