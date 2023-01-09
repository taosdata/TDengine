<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="dialog = true"
        size="small"
        icon="el-icon-plus"
        >{{ $t("add") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column :label="$t('topic.name')" prop="name"></el-table-column>
      <!-- <el-table-column
        :label="$t('topic.super')"
        prop="super"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.enable')"
        prop="enable"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.sysinfo')"
        prop="sysinfo"
      ></el-table-column> -->
      <el-table-column label="Permission">
        <template slot-scope="scope">
          <span>{{ scope | filterVal }}</span>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('topic.create_time')"
        prop="create_time"
      ></el-table-column>

      <el-table-column label="Action" width="65">
        <template slot-scope="scope">
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
        <el-form-item label="User Name" prop="user" required>
          <el-input v-model.trim="ruleForm.user"></el-input>
        </el-form-item>
        <el-form-item label="Password" prop="pwd" required>
          <el-input v-model.trim="ruleForm.pwd"></el-input>
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
export default {
  filters: {
    filterVal(val) {
      console.log(val, "filter");
      if (val.row.enable === 1) {
        return "Enable";
      }
      if (val.row.super === 1) {
        return "Super";
      }
      if (val.row.sysinfo === 1) {
        return "SysInfo";
      }
    },
  },
  computed:{
    confirmStatus(){
        if(!this.ruleForm.user){
            return true
        }
        if(!this.ruleForm.pwd){
            return true
        }
        return false
    }
  },
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {
        user: "",
        pwd: "",
      },
      rules: {
        user: [
          {
            message: "Please enter the user name",
            trigger: "blur",
          },
        ],
        pwd: [
          {
            message: "Please enter the password",
            trigger: "blur",
          },
        ],
      },
      topicList: [
        {
          name: "root",
          super: 0,
          enable: 0,
          sysinfo: 1,
          create_time: "2022-12-28 15:06:00.098",
        },
        {
          name: "root1",
          super: 1,
          enable: 0,
          sysinfo: 0,
          create_time: "2022-12-28 15:06:00.098",
        },
        {
          name: "root2",
          super: 0,
          enable: 1,
          sysinfo: 0,
          create_time: "2022-12-28 15:06:00.098",
        },
      ],
    };
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + '?', "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning",
      });
    },
  },
};
</script>
<style lang="scss" scoped>
</style>