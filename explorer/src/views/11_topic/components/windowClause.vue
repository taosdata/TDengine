<template>
  <div>
    <el-form-item :label="$t('stream.windowClause')">
      <el-radio-group size="small" v-model="window_clause.window_type">
        <el-radio-button label="SESSION"></el-radio-button>
        <el-radio-button label="STATE"></el-radio-button>
        <el-radio-button label="INTERVAL"></el-radio-button>
      </el-radio-group>
    </el-form-item>
    <el-form-item
      v-if="window_clause.window_type == 'SESSION'"
      :label="$t('sql.totalTime')"
    >
      <el-input-number
        v-model="window_clause.tol_val"
        :min="0"
      ></el-input-number>
      <el-select
        style="margin-left: 20px"
        v-model="window_clause.tol_unit"
        placeholder=""
      >
        <el-option
          v-for="item in timeUnit"
          :key="item.value"
          v-bind="item"
        ></el-option>
      </el-select>
    </el-form-item>
    <el-form-item
      v-if="window_clause.window_type == 'STATE'"
      :label="$t('stream.column')"
      prop="state_column"
      required
    >
      <el-select
        class="w100"
        v-model="window_clause.state_column"
        placeholder=""
      >
        <el-option
          v-for="item in stateColumn"
          :key="item.field"
          :value="item.field"
        ></el-option>
      </el-select>
    </el-form-item>
    <template v-if="window_clause.window_type == 'INTERVAL'">
      <el-form-item :label="$t('stream.intervalPeriod')">
        <el-input-number
          v-model="window_clause.interval_val"
          :min="1"
        ></el-input-number>
        <el-select
          style="margin-left: 20px"
          v-model="window_clause.interval_unit"
          placeholder=""
        >
          <el-option
            v-for="item in intervalTimeUnit"
            :key="item.value"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.intervaloffset')">
        <el-input-number
          v-model="window_clause.interval_offset"
          :min="0"
        ></el-input-number>
        <el-select
          style="margin-left: 20px"
          v-model="window_clause.offset_unit"
          placeholder=""
        >
          <el-option
            v-for="item in intervalTimeUnit"
            :key="item.value"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.slidingPeriod')">
        <template slot="label">
          <span>{{ $t("stream.slidingPeriod") }}&nbsp;</span>
          <el-tooltip
            effect="light"
            :content="$t('stream.slidingTip')"
            placement="top"
          >
            <i class="el-icon-info"></i>
          </el-tooltip>
        </template>
        <el-input-number
          v-model="window_clause.sliding_val"
          :min="0"
        ></el-input-number>
        <el-select
          style="margin-left: 20px"
          v-model="window_clause.sliding_unit"
          placeholder=""
        >
          <el-option
            v-for="item in timeUnit"
            :key="item.value"
            v-bind="item"
          ></el-option>
        </el-select>
      </el-form-item>
    </template>
  </div>
</template>

<script>
import { TDengineTimeUnit } from "@/const";
const stateColumnExculde = ["TIMESTAMP", "FLOAT", "DOUBLE"];
export default {
  props: {
    window_clause: {
      type: Object,
      default: () => {
        return {
          type: "SESSION",
          tol_val: "",
          tol_unit: "m",
          interval_val: "",
          interval_offset: "",
          column: "",
          interval_unit: "m",
          offset_unit: "m",
          sliding_val: "",
          sliding_unit: "s",
        };
      },
    },
    columnList: {
      type: Array,
      default: () => [],
    },
  },
  components: {},
  data() {
    return {
      timeUnit: TDengineTimeUnit,
      intervalTimeUnit: TDengineTimeUnit.slice(2),
    };
  },
  computed: {
    stateColumn() {
      return this.columnList.filter(
        (item) => !stateColumnExculde.includes(item.type)
      );
    },
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {},
};
</script>

<style scoped lang="scss">
:deep {
  .el-input-number__increase,
  .el-input-number__decrease {
    height: 30px;
    display: flex;
    justify-content: center;
    align-items: center;
  }
}
</style>
