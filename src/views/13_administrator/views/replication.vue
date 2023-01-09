<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="add"
        size="small"
        icon="el-icon-plus"
        >{{ $t("add") }}</el-button
      >
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
        :label="$t('topic.source')"
        prop="source"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.target')"
        prop="target"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.createdat')"
        prop="createdat"
      ></el-table-column>

      <el-table-column
        :label="$t('topic.stoppedat')"
        prop="stoppedat"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.status')"
        prop="status"
      ></el-table-column>

      <el-table-column label="Action" width="190">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="edit(scope.row)"
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
            @click="del(scope.row, scope.$index)"
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
      :title="$t('topic.addsource')"
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
        <el-form-item label="Source" prop="source" required>
          <el-input v-model.trim="ruleForm.source"></el-input>
        </el-form-item>
        <el-form-item label="Target" prop="target" required>
          <el-input v-model.trim="ruleForm.target"></el-input>
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
            @click="addReplication"
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
        source: "",
        target: "",
      },
      rules: {
        source: [
          {
            message: "Please enter the source",
            trigger: "blur",
          },
        ],
        target: [
          {
            message: "Please enter the target",
            trigger: "blur",
          },
        ],
      },
      topicList: [
        {
          no: 1,
          source: "test1",
          target: "target1",
          createdat: "---",
          stoppedat: "2022-12-29 15:06:00.098",
          status: "ready",
        },
        {
          no: 2,
          source: "test1",
          target: "target1",
          createdat: "---",
          stoppedat: "2022-12-29 15:06:00.098",
          status: "ready",
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
    add(){
        this.dialog=true
        this.ruleForm.source=''
        this.ruleForm.target=''
    },
    del(data) {
      this.$confirm("Are you sure  to delete " + data.source + '?', "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning",
      });
    },
    refresh(data) {
      console.log(data, "refresh");
    },
    addReplication() {},
    edit(data){
        this.dialog=true
        console.log(data,'edit')
        this.ruleForm.source=data.source
        this.ruleForm.target=data.target
    },
    start(data, index) {
      this.$set(this.topicList[index], "status", "Running");
      this.$set(
        this.topicList[index],
        "createdat",
        format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss")
      );
      console.log(data, "start");
    },
    stop(data, index) {
      this.$set(this.topicList[index], "status", "Ready");
      this.$set(
        this.topicList[index],
        "stoppedat",
        format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss")
      );
    },
  },
};
</script>
<style lang="scss" scoped>
</style>