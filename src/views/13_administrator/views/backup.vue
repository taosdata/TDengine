<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button plain @click="add" size="small" icon="el-icon-plus">{{
        $t("add")
      }}</el-button>
      <el-button plain @click="refresh" size="small" icon="el-icon-refresh">{{
        $t("refresh")
      }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column
        :label="$t('topic.no')"
        width="100"
        prop="no"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.databasetable')"
        prop="databasetable"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.backfile')"
        prop="backfile"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.createdat')"
        prop="createdat"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.status')"
        prop="status"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.laststart')"
        prop="laststart"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.laststop')"
        prop="laststop"
      ></el-table-column>

      <el-table-column label="Action" width="190">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="edit(scope.row, scope.$index)"
            icon="el-icon-edit"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="start(scope.row, scope.$index)"
            icon="el-icon-qidong"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="stop(scope.row, scope.$index)"
            icon="el-icon-tingzhi"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
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
      title="Create New Backup"
      width="600px"
      :visible.sync="dialog"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        class="demo-ruleForm"
      >
        <el-form-item label="Database" prop="db" required>
          <el-input v-model.trim="ruleForm.db"></el-input>
        </el-form-item>
        <el-form-item label="Directory" prop="file" required>
          <el-input v-model.trim="ruleForm.file"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            @click="addBack"
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
import { format } from "date-fns";
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {
        db: "",
        file: "",
      },
      rules: {
        db: [
          {
            message: "Please enter the database",
            trigger: "blur",
          },
        ],
        file: [
          {
            message: "Please check the file",
            trigger: "blur",
          },
        ],
      },
      topicList: [
        {
          no: 1,
          databasetable: "test1",
          backfile: "/abc/backfile.xml",
          createdat: "2022-12-28 15:06:00.098",
          status: "Ready",
          laststart: "2022-12-28 15:06:00.098",
          laststop: "2022-12-29 15:06:00.098",
        },
        {
          no: 2,
          databasetable: "test2",
          backfile: "/abc/backfile.xml",
          createdat: "2022-12-28 15:06:00.098",
          status: "Ready",
          laststart: "2022-12-28 15:06:00.098",
          laststop: "2022-12-29 15:06:00.098",
        },
      ],
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.source) {
        return true;
      }
      if (!this.ruleForm.target) {
        return true;
      }
      return false;
    },
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.databasetable + '?', "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning",
      });
    },
    add() {
      this.dialog = true;
      this.ruleForm.db = "";
      this.ruleForm.file = "";
    },
    refresh() {},
    edit(data) {
      this.dialog = true;
      this.ruleForm.db = data.databasetable;
      this.ruleForm.file = data.backfile;
    },
    start(data, index) {
      this.$set(this.topicList[index],'status','Running')
      this.$set(this.topicList[index],'createdat',format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss"))
      this.$set(this.topicList[index],'laststart',format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss"))
    },
    stop(data, index) {
      this.$set(this.topicList[index],'status','Ready')
      this.$set(this.topicList[index],'laststop',format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss"))
    },
    addBack() {},
  },
};
</script>
<style lang="scss" scoped>
</style>