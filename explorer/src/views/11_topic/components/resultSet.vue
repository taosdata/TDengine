<template>
  <div>
    <el-table
      tooltip-effect="dark"
      style="width: 100%"
      :data="value"
      size="mini"
    >
      <el-table-column width="40">
        <template slot-scope="scope">
          <el-checkbox v-model="scope.row.checked"></el-checkbox>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('name')"
        show-overflow-tooltip
        prop="name"
        min-width="120"
      >
      </el-table-column>
      <el-table-column
        prop="type"
        :label="$t('type')"
        show-overflow-tooltip
        width="120"
      >
      </el-table-column>
      <el-table-column prop="result" :label="$t('topic.resultSet')" width="120">
        <template slot-scope="{ row, $index }">
          <el-button
            :disabled="!row.fnList"
            @click="result(row, $index)"
            icon="el-icon-setting"
            size="mini"
          ></el-button>
        </template>
      </el-table-column>
      <el-table-column
        prop="condition"
        :label="$t('topic.conditionSet')"
        width="120"
      >
        <template slot-scope="{ row, $index }">
          <el-button
            @click="result(row, $index, 1)"
            :disabled="!row.conditionList.length"
            icon="el-icon-setting"
            size="mini"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog
      append-to-body
      width="400px"
      center
      :title="title"
      :visible.sync="dialog"
      :close-on-click-modal="false"
    >
      <component :is="comp" v-bind="dialogParams" :field="field"></component>
      <section slot="footer">
        <el-button @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button @click="confirm" type="primary">{{
          $t("confirm")
        }}</el-button>
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
  CoversionFn,
  DatetimeFN,
  AggregationFn,
  SelectorFn,
  SeriesSpecificFn,
  SystemFn,
  TDengineStringType,
  TDengineNumberType,
  CompareOperator,
  JsonOperator,
  GeneralOperator,
  RegularOperator
} from "@/const";
import { isArray } from "@/utils/validate";
const fnMap = {
  NUMBER: NumbericFn,
  STRING: StringFn,
  COVERSION: CoversionFn,
  DATETIME: DatetimeFN,
  AVGFN: AggregationFn,
  SELECTION: SelectorFn,
  SERIES: SeriesSpecificFn,
  SYSTEM: SystemFn,
};
const fnMapName = new Map([
  ["NUMBER", "NumbericFn"],
  ["STRING", "StringFn"],
  ["COVERSION", "CoversionFn"],
  ["DATETIME", "DatetimeFN"],
  ["AVGFN", "AggregationFn"],
  ["SELECTION", "SelectorFn"],
  ["SERIES", "SeriesSpecificFn"],
  ["SYSTEM", "SystemFn"],
]);
const getGeneralFn = (type) => {
  return GeneralOperator.filter(
    (item) => !type.includes(item.label ) 
  ).map((item) => item.label);
};
const conditionMap = {
  TIMESTAMP: CompareOperator.concat(getGeneralFn(["TIMESTAMP"])),
  NUMBER: CompareOperator.concat(getGeneralFn(["NUMBER"])),
  STRING: RegularOperator.concat(getGeneralFn(["STRING"])),
  JSON: JsonOperator,
  BOOL: CompareOperator.concat(getGeneralFn(["NOT BETWEEN AND", "BETWEEN AND" ])),
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
      field: ''
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
      if (
        !this.params.selected_db ||
        (!this.params.selected_tb && !this.params.stableName)
      )
        return this.$emit("change", []);
      const dataFn = this.params.stableName
        ? getStableStructReq
        : getMatrixStructReq;
      dataFn(this.params)
        .then((data) => {
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
              // fnList: fnMap.AVGN,
              checked: true,
              conditionList: [],
              condition: [],
              fnList: this.loadAllFns(),
            });
          }
          try {
            fields.forEach((item) => {
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
                    value1: "",
                    key: 1,
                    operator: "",
                  },
                ],
                checked: true,
                // fnList,
                fnList: this.loadAllFns(),
                conditionList: conditionMap[this.getType(item.type)],
              });
            });
          } catch (error) {
            console.log(error);
          }
          this.$emit("change", result);
        })
        .catch(() => (this.options = []));
    },
    handleFnParamsFiled(fnList = []) {
      const result = {
        fn: "",
      };
      fnList.forEach((item) => {
        if (item.filters) {
          result.params = {};
          item.filters.forEach((it) => {
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
      row.fnList
        .map((item) => item.options)
        .flat(1)
        .map((val) => {
          let type = row.type?.toLowerCase().includes("varchar")
            ? "varchar"
            : row.type?.toLowerCase().includes("nchar")
            ? "nchar"
            : row.type?.toLowerCase();
          if (!val.supportDatatype.includes(type)) {
            if (
              val.supportDatatype[0] == "all" ||
              val.supportDatatype[0] == "system"
            ) {
              val["selectDisable"] = false;
            } else {
              val["selectDisable"] = true;
            }
          } else {
            val["selectDisable"] = false;
          }
        });

      let result = deepClone(row);
      this.dialogParams = result;
      this.field = result.name;
    },

    confirm() {
      switch (this.dialogType) {
        case 0:
          this.value[this.currentRowIndex].result = this.dialogParams.result;
          break;
        case 1:
          this.value[this.currentRowIndex].condition =
            this.dialogParams.condition;
          break;
        default:
          break;
      }
      this.dialog = false;
    },
    //加载官网提供的所有函数
    loadAllFns() {
      let result = [];
      result = Object.keys(fnMap).map((key) => {
        return {
          label: this.$t(`explorerfns.${fnMapName.get(key)}`),
          options: fnMap[key],
        };
      });
      return result;
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
