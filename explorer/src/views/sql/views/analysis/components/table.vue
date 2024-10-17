<template>
  <el-table
    :data="data"
    header-cell-class-name="header-class"
    class="table-style"
  >
    <el-table-column prop="date" min-width="280">
      <template slot="header">
        <span>
          <span>{{ $t("sql.sqlTemplate") }}</span>
          <el-icon class="el-icon-warning-outline" />
        </span>
      </template>
      <template slot-scope="scope">
        <el-popover placement="right" width="400" trigger="hover">
          sadasdasdasdqwdqwasdoqwodqw
          <pre
            class="nowrap"
            slot="reference"
            v-highlight
          ><code>{{ scope.row.code }}</code></pre>
        </el-popover>
      </template>
    </el-table-column>
    <el-table-column prop="name" sortable width="200">
      <template slot="header">
        <span>
          <span>{{ $t("sql.cumulativeTime") }}</span>
          <el-icon class="el-icon-warning-outline" />
        </span>
      </template>
    </el-table-column>
    <el-table-column prop="address" width="150">
      <template slot="header">
        <span>
          <span>{{ $t("sql.averageTime") }}</span>
          <el-icon class="el-icon-warning-outline" />
        </span>
      </template>
    </el-table-column>
    <el-table-column prop="address" width="200">
      <template slot="header">
        <span>
          <span>{{ $t("sql.execNum") }}</span>
          <el-icon class="el-icon-warning-outline" />
        </span>
      </template>
    </el-table-column>
  </el-table>
</template>

<script>
export default {
  props: {
    data: {
      type: Array,
      default: () => []
    }
  }
};
</script>

<style lang="scss" scoped>
.table-style {
  width: 100%;
  margin-top: 20px;
}
</style>
<style lang="scss">
.header-class {
  color: #000;
  span {
    padding-right: 5px;
  }
  p {
    display: flex;
  }
}
</style>
