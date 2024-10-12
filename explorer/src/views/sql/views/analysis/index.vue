<template>
  <div class="analysis">
    <el-form :inline="true" size="mini" :model="searchParams">
      <el-form-item>
        <el-date-picker
          type="datetime"
          v-model="searchParams.test"
          :picker-options="$root.pickerOptions"
          :placeholder="$t('sql.periodTip')"
        >
        </el-date-picker>
      </el-form-item>
      <el-form-item>
        <el-select
          v-model="searchParams.test"
          :placeholder="$t('sql.databaseTip')"
        >
          <el-option label="区域一" value="shanghai"></el-option>
          <el-option label="区域二" value="beijing"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-select v-model="searchParams.test" :placeholder="$t('sql.typeTip')">
          <el-option label="区域一" value="shanghai"></el-option>
          <el-option label="区域二" value="beijing"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-input v-model="searchParams.test" :placeholder="$t('sql.approver')">
          <el-button slot="append" icon="el-icon-search"></el-button>
        </el-input>
      </el-form-item>
    </el-form>
    <tableCom :data="tableData" />
  </div>
</template>

<script>
import tableCom from "./components/table.vue";
export default {
  components: { tableCom },
  data() {
    return {
      searchParams: {
        test: ""
      },
      tableData: [
        {
          code: "SELECT * FROM stmt_type"
        }
      ]
    };
  }
};
</script>

<style scoped></style>
