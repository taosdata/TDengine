<template>
  <div class="">
    <el-radio-group
      v-model="radioValue"
      :disabled="disabled()"
    >
      <el-radio
        v-for="item in radioList"
        :key="item.display"
        :label="item.type == 'time' ? null : item.value"
      >
        <TimezoneDatePicker
          v-if="item.type == 'time'"
          v-model="dateTime"
          :disabled="data[config.field] == 'auto'"
          value-format="yyyy-MM-dd HH:mm:ss"
          :placeholder="item.display"
          type="datetime"
          @click.native.stop
          @change="timeChange"
        ></TimezoneDatePicker>
        <span v-else>
          {{ item.display }}
        </span>
      </el-radio>
    </el-radio-group>
  </div>
</template>

<script>
import TimezoneDatePicker from '@/components/date-picker';
export default {
  props: {
    data: {
      type: Object,
      default: () => ({})
    },
    config: {
      type: Object,
      default: () => ({})
    }
  },
  inject: ['sourceParent'],
  components: { TimezoneDatePicker },
  data() {
    return {
      dateTime: ''
    };
  },
  computed: {
    formDisabled() {
      return this.sourceParent.formDisabled;
    },
    radioList() {
      return this.config?.options ?? [];
    },
    radioValue: {
      get() {
        return this.data[this.config.field] == 'auto' ? 'auto' : null;
      },
      set(val) {
        this.data[this.config.field] = val == 'auto' ? 'auto' : this.dateTime;
      }
    }
  },
  watch: {},
  created() {
    if (this.data[this.config.field] !== 'auto') {
      this.dateTime = this.data[this.config.field];
    }
  },
  mounted() {},
  methods: {
    timeChange(val) {
      this.data[this.config.field] = val;
    },
    disabled() {
      if (this.formDisabled) return true;
      if (this.config.disabled && typeof this.config.disabled == 'function') {
        return this.config.disabled(this.data);
      } else {
        return false;
      }
    }
  }
};
</script>

<style scoped lang="scss">
:deep(.el-radio) {
  display: block;
  & + .el-radio {
    margin-top: 10px;
  }
}
</style>
