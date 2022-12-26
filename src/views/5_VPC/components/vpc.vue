<template>
  <div>
    <el-table size="mini">
      <el-table-column
        v-for="item in columns"
        :key="item.label"
        :label="$t(item.label)"
        :min-width="$t(item.label).length * 10"
        :prop="item.field || item.label"
      />
    </el-table>
  </div>
</template>

<script>
import columns from "./column";
export default {
  props: {
    type: {
      type: String,
      default: "",
    },
  },
  components: {},
  computed: {
    columns() {
      console.log(columns);
      return columns[this.title];
    },
    title() {
      return this.$store.state.app.cloud;
    },
  },
};
</script>

<style></style>
