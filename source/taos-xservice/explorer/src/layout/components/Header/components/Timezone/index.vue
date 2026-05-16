<template>
  <div class="timezone-wrapper">
    <div class="timezone-block">
      <el-select
        v-model="timezone"
        size="default"
        filterable
        popper-class="timezone-select"
        style="width: 200px"
        @change="handChange"
      >
        <el-option v-for="item of timezones" :key="item" :value="item">
          <div class="timezone-item">
            <span>{{ item }}</span>
            <span>{{ getAdditions(item) }}</span>
          </div>
        </el-option>
      </el-select>
    </div>
  </div>
</template>

<script setup lang="ts">
import moment from 'moment-timezone';
import { useStore } from 'vuex';
import { setTimezone as setTaosUiTimezone } from 'taos-ui/config';
const store = useStore();

const timezone = ref(localStorage.getItem('timezone') || moment.tz.guess(true)); // 获取浏览器的时区
const timezones = moment.tz.names();

onMounted(() => {
  setTimezone(timezone.value);
});

function handChange(value: string) {
  setTimezone(value);
}
function getAdditions(string: string) {
  return moment.tz(string).format('Z');
}
function setTimezone(value: string) {
  setTaosUiTimezone(value);
  localStorage.setItem('timezone', value);
  store.commit('app/SET_TIME_ZONE', value);
}
</script>

<style lang="scss">
.timezone-select {
  z-index: 99999999 !important;
}
</style>

<style lang="scss" scoped>
.timezone-wrapper {
  position: relative;
  margin-top: 4px;
  margin-right: 10px;
  cursor: pointer;
}

.timezone-block {
  display: flex;
  align-items: center;

  > i {
    font-size: 30px;
    color: $color-primary;
  }
}

.timezone-item {
  display: flex;
  justify-content: space-between;
}

// select
:deep(.el-input--default .el-input__inner) {
  color: $color-primary;

  // border: 1px solid $color-primary;
}

:deep(.el-select .el-input .el-select__caret) {
  color: $color-primary;
}

:deep(.el-select-dropdown__item) {
  font-size: 12px;
}
</style>
