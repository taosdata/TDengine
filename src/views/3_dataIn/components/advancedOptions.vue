<template>
  <div class="advanced-options" v-if="options">
    <div class="advanced-title">
      <div class="block-title">
        <span>{{ options?.name }}</span>
        <el-switch
          v-model="options.collapsed"
          style="margin-left: 20px"
          @change="switchOption"
        ></el-switch>
      </div>
      <div class="description">
        {{ options?.description }}
      </div>
    </div>
    <el-form label-width="200px" v-if="showOptions">
      <template v-for="(item, index) in options.params">
        <el-form-item
          v-if="!item.hidden"
          :key="index"
          :prop="item.name"
          :label="item?.display"
          :class="[
            'visible',
            item?.requires !== 'keep_raw_data'
              ? 'show'
              : showRawdata
              ? 'show'
              : 'hidden',
          ]"
        >
          <template v-if="item.hint?.type == 'str' && item.hint.choices">
            <el-select
              v-model="item.value"
              size="small"
              :palceholder="item?.placeholder"
            >
              <template v-for="(val, ind) in item.hint.choices">
                <el-option :label="val" :value="val" :key="ind"></el-option>
              </template>
            </el-select>
          </template>
          <template v-if="item.hint?.type == 'str' && !item.hint.choices">
            <el-input
              v-model="item.value"
              size="small"
              :placeholder="item?.placeholder"
            ></el-input>
          </template>
          <template v-if="item.hint.type == 'integer'">
            <el-input-number
              v-model="item.value"
              :min="item.hint?.min"
              :max="item.hint?.max"
              size="small"
            ></el-input-number>
          </template>
          <template v-if="item.hint.type == 'bool'">
            <el-switch
              v-model="item.value"
              @change="switchChange($event, item.name)"
            >
            </el-switch>
          </template>

          <p class="description">{{ item?.description }}</p>
        </el-form-item>
      </template>
    </el-form>
  </div>
</template>
<script>
export default {
  name: "AdvancedOptions",
  props: {
    options: {
      type: Object,
      default: () => {
        return null;
      },
    },
  },
  data() {
    return {
      rule: {},
      showRawdata: false,
      showOptions: false,
      paramStr: "",
    };
  },
  mounted() {
    this.showOptions = this.options.collapsed;
  },
  methods: {
    switchChange(val, data) {
      this.showRawdata = val;
    },
    switchOption(val) {
      this.showOptions = val;
    },
  },
  watch: {
    options: {
      deep: true,
      handler(val) {
        //拼接参数到dns上
        if (val.collapsed) {
          //展开时候才会传参数
          this.paramStr = "";
          this.paramStr += "&collapsed=true&";
          let realArr=val.params.filter(val=>!val.hidden)
          realArr.forEach((item, index) => {
            if (item?.requires != "keep_raw_data") {
              this.paramStr +=
                `${item.name}=${
                  item.name == "keep_raw_data" &&
                  Object.is(item.value, undefined)
                    ? false
                    : item.value
                }` + (index < realArr.length - 3 ? "&" : "");
            } else {
              if (this.showRawdata) {
                this.paramStr +=
                  `&${item.name}=${item.value}` +
                  (index < val.params.length - 1 ? "&" : "");
              }
            }
          });
          this.$emit("sendAdvanceParams", this.paramStr);
        }
      },
    },
  },
};
</script>
<style lang="scss" scoped>
.advanced-title {
  margin-bottom: 10px;
}
::v-deep {
  .el-form-item__content {
    display: flex;
    flex-direction: column;
  }
  .description {
    margin-top: 8px;
  }
  .el-form-item.visible.show {
    display: block;
  }
  .el-form-item.visible.hidden {
    display: none;
  }
}
</style>
