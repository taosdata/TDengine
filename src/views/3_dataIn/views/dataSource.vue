<template>
  <div class="data-source">
    <div class="flexEnd">
      <el-button
        class="big-button"
        plain
        @click="dialog = true"
        size="small"
        icon="el-icon-plus"
        >{{ $t("topic.addsource") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column
        :label="$t('topic.data_source_name')"
        prop="data_source_name"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.status')"
        prop="status"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.create_time')"
        prop="create_time"
      ></el-table-column>

      <el-table-column label="Action" width="180" class="action">
        <template slot-scope="scope">
          <el-button
            type="danger"
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
          <el-button
            type="primay"
            size="small"
            @click="del(scope.row)"
            icon="el-icon-more"
            style="border-color: #409eff"
          ></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
    <el-dialog
      align="center"
      :title="$t('topic.addsource')"
      width="600px"
      :visible.sync="dialog"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        label-width="150px"
        class="demo-ruleForm"
      >
        <el-form-item label="Source Name" prop="name">
          <el-input v-model="ruleForm.name"></el-input>
        </el-form-item>
        <el-form-item label="Status" prop="status">
          <el-select
            v-model="ruleForm.status"
            placeholder="Please select status"
          >
            <el-option label="Pending" value="pending"></el-option>
            <el-option label="Fullfiled" value="fullfiled"></el-option>
            <el-option label="Rejected" value="rejected"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Created Time" required>
          <el-form-item prop="time">
            <el-date-picker
              v-model="ruleForm.time"
              type="datetime"
              placeholder="选择日期时间"
            >
            </el-date-picker>
          </el-form-item>
        </el-form-item>
      </el-form>
      <el-row style="margin-top: 20px">
        <el-col :span="5" offset="6">
          <el-button size="small" @click="dialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="requestIng || !sql"
            @click="handleCreateTopic"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script>
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {
        name: "",
        status: "",
        time: "",
      },
      topicList: [
        {
          id: 1,
          data_source_name: "InfluxDB",
          status: "Pending",
          create_time: "2022-10-10 12:02:10",
        },
        {
          id: 2,
          data_source_name: "OpenTSDB",
          status: "Fullfiled",
          create_time: "2022-10-20 12:02:10",
        },
      ],
    };
  },
  methods: {
    handlePageChange() {},
  },
};
</script>
<style lang="scss" scoped>
::v-deep.el-form-item__label {
  white-space: nowrap !important;
}
.el-form-item {
  display: flex;
}
.el-form-item__content {
  margin-left: 0px !important;
}
::v-deep.el-input__inner {
  width: 300px;
}
</style>