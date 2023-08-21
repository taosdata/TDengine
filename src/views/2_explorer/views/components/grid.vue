<template>
  <div class="gird">
    <!-- 列表 -->
    <el-table
      :key="key"
      stripe
      tooltip-effect="light"
      size="mini"
      v-load-more.expand.immediate="{
        func: load,
        target: '.el-table__body-wrapper',
        delay: 200,
        distance: 100,
      }"
      :data="currentTableData"
      height="100%"
      style="border-bottom: none"
    >
      <!--数据源-->

      <template v-if="head.length">
        <el-table-column
          v-for="(field, index) of head"
          :key="field"
          :prop="index + ''"
          min-width="170px"
          :show-overflow-tooltip="true"
          :label="field"
        >
        </el-table-column>
      </template>
    </el-table>
  </div>
</template>
<script>
import { mapState } from "vuex";
export default {
  name: "grid",
  data() {
    return {
      currentTableData: [],
      currentPage: 1,
      pageSize: 30,
      key: 0,
    };
  },
  components: {},
  computed: {
    ...mapState({
      dataSource: (state) => state.console.result,
      head: (state) => state.console.head,
    }),
    headMap() {
      if (Array.isArray(this.head)) {
        return this.head.reduce((map, key, index) => {
          map[key] = index;
          return map;
        }, {});
      }
      return {};
    },
  },
  watch: {
    dataSource: {
      handler(val) {
        this.key++;
        this.currentTableData = val.slice(0, this.pageSize).map((item) => {
          const obj = {};
          for (const key in item) {
            obj[this.headMap[key]] = item[key];
          }
          return obj;
        });
        this.currentPage = 1;
      },
      immediate: true,
    },
  },
  mounted() {},
  methods: {
    load() {
      if (this.currentTableData.length === this.dataSource.length) return;
      this.currentPage++;
      this.currentTableData.push(
        ...this.dataSource
          .slice(
            this.pageSize * (this.currentPage - 1),
            this.pageSize * this.currentPage
          )
          .map((item) => {
            const obj = {};
            for (const key in item) {
              obj[this.headMap[key]] = item[key];
            }
            return obj;
          })
      );
    },
  },
};
</script>
<style lang="scss" scoped>
.gird {
  height: 100%;
  overflow: auto;
  overflow-x: hidden;
  &:deep(.el-table::before) {
    height: 0;
  }
}
</style>
