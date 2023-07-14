<template>
  <div class="timezone_wrapper">
    <div class="timezone_block">
      <el-select v-model="timezone" @change="handChange" size="small" filterable>
        <el-option v-for="item of timezones" :value="item" :key="item">
          <div class="timezone_item">
            <span>{{ item }}</span>
            <span>{{ getAdditions(item) }}</span></div>
        </el-option>
      </el-select>
    </div>
  </div>
</template>

<script>
  import moment from 'moment-timezone';
  export default {
    name: "timezone",         
    data() {
      return {
        timezone: moment.tz.guess(true),// 获取浏览器的时区
        timezones: moment.tz.names(),
      };
    },
    mounted() {
      localStorage.setItem("timezone", this.timezone)
    },
    computed: {},
    methods: {
      handChange(value) {
        this.isShow = false 
        localStorage.setItem("timezone", value)
        this.$store.commit("app/SET_TIME_ZONE", value);
      },
      getAdditions(string) {
        return moment.tz(string).format("Z")
      }
    },
  };
</script>

<style lang="scss" scoped>
  .timezone_wrapper {
    cursor: pointer;
    margin-right: 10px;
    margin-top: 4px;
    position: relative;
  }
  .timezone_block {
    display: flex;
    align-items: center;
    > i {
      color: $color-primary;
      font-size: 30px;
    }
  }
  .timezone_item {
    display: flex;
    justify-content: space-between;
  }
 
  // select
  ::v-deep .el-input--small .el-input__inner {
    color: $color-primary;
    // border: 1px solid $color-primary;
  }
  ::v-deep .el-select .el-input .el-select__caret {
    color: $color-primary;
  }
  ::v-deep .el-select-dropdown__item {
    font-size: 12px;
  }


  

</style>
