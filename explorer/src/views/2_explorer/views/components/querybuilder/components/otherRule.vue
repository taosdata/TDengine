<template>
  <div>
    <el-row class="row-style">
      <el-col :span="12" class="col-style">
        <span class="label">ORDER BY</span>
        <el-input 
          v-model="otherRule.orderby" 
          placeholder="" 
          style="margin-right: 8px; width: 280px;" 
          :size="size"
          controls-position="right"
          >
        </el-input>
      </el-col>
    </el-row>
    <el-row class="row-style">
      <el-col :span="12" class="col-style">
        <span class="label">LIMIT</span>
        <el-input-number 
          v-model="otherRule.limit" 
          placeholder="" 
          style="margin-right: 8px; width: 280px;" 
          :size="size"
          :min="0"
          controls-position="right"
          >
        </el-input-number>
      </el-col>
      <el-col :span="12" class="col-style">
        <span class="label">OFFSET</span>
        <el-input-number 
          v-model="otherRule.offset" 
          placeholder="" 
          style="margin-right: 8px; width: 280px;" 
          :size="size"
          :min="0"
          controls-position="right"
          >
        </el-input-number>
      </el-col>
    </el-row>
    <el-collapse v-model="activeNames" class="row-style">
      <el-collapse-item title="GROUP BY / PARTITION BY" name="1">
        <el-row class="row-style">
          <el-col :span="12" class="col-style">
            <span class="label">GROUP BY</span>
            <el-input 
              v-model="otherRule.groupby" 
              placeholder="" 
              style="margin-right: 8px; width: 280px;" 
              :size="size"
              :disabled="otherRule.partitionby != ''"
              >
            </el-input>
          </el-col>
          <el-col :span="12" class="col-style">
            <span class="label">HAVING</span>
            <el-input 
              v-model="otherRule.having" 
              placeholder="" 
              style="margin-right: 8px; width: 280px;" 
              :size="size"
              :disabled="otherRule.partitionby != ''"
              >
            </el-input>
          </el-col>
        </el-row>
        <el-row class="row-style">
          <el-col :span="12" class="col-style">
            <span class="label">PARTITION BY</span>
            <el-input 
              v-model="otherRule.partitionby" 
              placeholder="" 
              style="margin-right: 8px; width: 280px;" 
              :size="size"
              :disabled="otherRule.groupby != '' || otherRule.having != ''"
              >
            </el-input>
          </el-col>
        </el-row>
        <!-- slimit 和 PARTITION BY/GROUP BY 搭配使用 start -->
        <el-row class="row-style">
          <el-col :span="12" class="col-style">
            <span class="label">SLIMIT</span>
            <el-input-number 
              v-model="otherRule.slimit" 
              placeholder="" 
              style="margin-right: 8px; width: 280px;" 
              :size="size"
              :min="0"
              controls-position="right"
              >
            </el-input-number>
          </el-col>
          <el-col :span="12" class="col-style">
            <span class="label">SOFFSET</span>
            <el-input-number 
              v-model="otherRule.soffset" 
              placeholder="" 
              style="margin-right: 8px; width: 280px;" 
              :size="size"
              :min="0"
              controls-position="right"
              >
            </el-input-number>
          </el-col>
        </el-row>
        <!-- slimit 和 PARTITION BY/GROUP BY 搭配使用 end -->
      </el-collapse-item>
      <el-collapse-item title="Window Clause" name="3">
        <el-row class="row-style">
          <el-col :span="12">
            <el-col class="col-style">
              <span class="label">{{ $t('stream.windowClause') }}</span>
              <el-radio-group :size="size" v-model="otherRule.window_type">
                <el-radio-button label="SESSION"></el-radio-button>
                <el-radio-button label="STATE"></el-radio-button>
                <el-radio-button label="INTERVAL"></el-radio-button>
                <el-radio-button label="EVENT"></el-radio-button>
              </el-radio-group>
            </el-col>
            <el-col
              v-if="otherRule.window_type == 'SESSION'"
              class="col-style row-style"
            >
              <span class="label">{{ $t('sql.totalTime') }}</span>
              <el-input-number
                v-model="otherRule.tol_val"
                :min="0"
                :size="size"
                controls-position="right"
              ></el-input-number>
              <el-select
                style="margin-left: 20px; width: 130px"
                v-model="otherRule.tol_unit"
                placeholder=""
                :size="size"
              >
                <el-option
                  v-for="item in timeUnit"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
            </el-col>
            <el-col
              v-if="otherRule.window_type == 'STATE'"
              prop="state_column"
              class="col-style row-style"
            >
              <span class="label">{{ $t('stream.column') }}</span>
              <el-input
                v-model="otherRule.state_column"
                placeholder=""
                :size="size"
                style="width: 280px;"
              >
              </el-input>
            </el-col>
            <template v-if="otherRule.window_type == 'INTERVAL'">
              <el-col class="col-style row-style">
                <span class="label">{{ $t('stream.intervalPeriod') }}</span>
                <el-input-number
                  v-model="otherRule.interval_val"
                  :min="1"
                  :size="size"
                  controls-position="right"
                ></el-input-number>
                <el-select
                  style="margin-left: 20px; width: 130px"
                  v-model="otherRule.interval_unit"
                  placeholder=""
                  :size="size"
                >
                  <el-option
                    v-for="item in intervalTimeUnit"
                    :key="item.value"
                    v-bind="item"
                  ></el-option>
                </el-select>
              </el-col>
              <el-col class="col-style row-style">
                <span class="label">{{ $t('stream.intervaloffset') }}</span>
                <el-input-number
                  v-model="otherRule.interval_offset"
                  :min="0"
                  :size="size"
                  controls-position="right"
                ></el-input-number>
                <el-select
                  style="margin-left: 20px; width: 130px"
                  v-model="otherRule.offset_unit"
                  placeholder=""
                  :size="size"
                >
                  <el-option
                    v-for="item in intervalTimeUnit"
                    :key="item.value"
                    v-bind="item"
                  ></el-option>
                </el-select>
              </el-col>
              <el-col class="col-style row-style">
                <span class="label">{{ $t("stream.slidingPeriod") }}&nbsp;
                  <el-tooltip
                    effect="light"
                    :content="$t('stream.slidingTip')"
                    placement="top"
                  >
                    <i class="el-icon-info"></i>
                  </el-tooltip>
                </span>
                <el-input-number
                  v-model="otherRule.sliding_val"
                  :min="0"
                  :size="size"
                  controls-position="right"
                ></el-input-number>
                <el-select
                  style="margin-left: 20px; width: 130px"
                  v-model="otherRule.sliding_unit"
                  placeholder=""
                  :size="size"
                >
                  <el-option
                    v-for="item in timeUnit"
                    :key="item.value"
                    v-bind="item"
                  ></el-option>
                </el-select>
              </el-col>
            </template>
            <template v-if="otherRule.window_type == 'EVENT'">
              <el-col
                class="col-style row-style"
              >
                <span class="label">START WITH</span>
                <el-input
                  v-model="otherRule.start_with"
                  :size="size"
                  style="width: 280px"
                ></el-input>
              </el-col>
              <el-col
                class="col-style row-style"
              >
                <span class="label">END WITH</span>
                <el-input
                  v-model="otherRule.end_with"
                  :size="size"
                  style="width: 280px"
                ></el-input>
              </el-col>
            </template>
    
          </el-col>
        </el-row>
      </el-collapse-item>
      <el-collapse-item title="INTERP" name="4" v-if="isInterp">
        <!-- interp start -->
        <el-row class="row-style">
          <el-col class="col-style">
            <span class="label flex-none">INTERP</span>
            <el-row>
              <el-col>
                <span class="label">RANGE</span>
                <DatePicker 
                  v-model="otherRule.range1"
                  :size="size" 
                  type="datetime"
                  value-format="yyyy-MM-dd HH:mm:ss"
                  style="width: 130px;">
                </DatePicker>
                <span class="w20">~</span>
                <DatePicker 
                  v-model="otherRule.range2"
                  :size="size" 
                  type="datetime"
                  value-format="yyyy-MM-dd HH:mm:ss"
                  style="width: 130px;">
                </DatePicker>
              </el-col>
              <el-col class="">
                <span class="label">EVERY</span>
                <el-input-number
                  v-model="otherRule.every_val"
                  :min="0"
                  :size="size"
                  controls-position="right"
                  style="width: 130px;"
                ></el-input-number>
                <el-select
                  style="margin-left: 20px;width: 130px;"
                  v-model="otherRule.every_unit"
                  placeholder=""
                  :size="size"
                >
                  <el-option
                    v-for="item in timeUnit"
                    :key="item.value"
                    v-bind="item"
                  ></el-option>
                </el-select>
              </el-col>
              <el-col class="">
                <span class="label">FILL</span>
                <el-select
                  v-model="otherRule.fill"
                  placeholder=""
                  :size="size"
                  style="width: 130px;"
                >
                  <el-option
                    v-for="item in fillClause"
                    :key="item"
                    :label="item"
                    :value="item"
                  ></el-option>
                </el-select>
                <el-input
                  v-if="otherRule.fill === 'VALUE'"
                  v-model="otherRule.fill_val"
                  :size="size"
                  style="margin-left: 20px; width: 130px;"
                ></el-input>
              </el-col>
            </el-row>
          </el-col>
        </el-row>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script>
import { TDengineTimeUnit, TDengineFill } from "@/const";
import DatePicker from '@/components/date-picker'
const stateColumnExculde = ["TIMESTAMP", "FLOAT", "DOUBLE"];

export default {
  name: 'OtherRule',
  components: { DatePicker },
  data() {
    return {
      activeNames: '1',
      columns: [],
      timeUnit: TDengineTimeUnit,
      intervalTimeUnit: TDengineTimeUnit.slice(2),
      fillClause: TDengineFill
    }
  },
  props: {
    otherRule: {
      type: Object,
      default: () => {}
    },
    size: {
      type: String,
      default: 'mini'
    },
    columnList: {
      type: Array,
      default: () => [],
    },
    general: {
      type: Object,
      default : () => {}
    },
    isInterp: {
      type: Boolean,
      default: () => false
    }
  },
  computed: {
    stateColumn() {
      return this.columnList.filter(
        (item) => !stateColumnExculde.includes(item.type)
      );
    }
  }
}
</script>

<style lang="scss" scoped>
.col-style {
  display: flex;
  align-items: center;
}
.row-style {
  margin-top: 10px;
}
.flex-none {
  flex: none;
}
.w20 {
  width: 20px;
  display: inline-block;
  text-align: center;
}

::v-deep .el-radio-button--mini .el-radio-button__inner {
  padding: 7px 13px !important;
}
::v-deep .el-collapse-item__header {
  color: #4259ce;
  font-size: 14px;
}
</style>