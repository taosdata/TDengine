<template>
  <div class="mnode-block">
<<<<<<< HEAD
    <div class="flexEnd">
      <el-button plain @click="add" size="small" icon="el-icon-plus">{{
        $t("add")
      }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column
        :label="$t('topic.endpoint')"
        prop="endpoint"
      ></el-table-column>
      <el-table-column :label="$t('topic.role')" prop="role"></el-table-column>
      <el-table-column
        :label="$t('topic.status')"
        prop="status"
      ></el-table-column>
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
=======
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column  :label="$t('topic.endpoint')" prop="endpoint"></el-table-column>
      <el-table-column  :label="$t('topic.role')" prop="role"></el-table-column>
      <el-table-column  :label="$t('topic.status')" prop="status"></el-table-column>
      <el-table-column  :label="$t('topic.create_time')" prop="create_time"></el-table-column>
      
      <el-table-column label="Action" width="80">
        <template slot-scope="scope">
          <el-button type="danger" plain size="small" @click="del(scope.row)" icon="el-icon-delete"></el-button>
>>>>>>> 8895288015f78987fe1192eeaa34d5fe7903a71c
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
<<<<<<< HEAD
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
        <el-form-item label="End Point" prop="endpoint" required>
          <el-input v-model.trim="ruleForm.endpoint"></el-input>
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
            @click="addMnodes"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
=======
>>>>>>> 8895288015f78987fe1192eeaa34d5fe7903a71c
  </div>
</template>
<script>
import mix from "./mix";
export default {
<<<<<<< HEAD
  mixins: [mix],
  data() {
    return {
      topicList: [
        {
          id: 1,
          endpoint: "huolinhe:6030",
          role: "leader",
          status: "ready",
          create_time: "2022-12-28 15:06:00.098",
        },
      ],
    };
  },
  methods: {
    handlePageChange() {},
    addMnodes(){}
    
  },
=======
  data() {
    return {
      pageSize:10,
      currentPage:1,
      total:10,
      topicList:[]
    };
  },
  methods:{
    handlePageChange(){}
  }
>>>>>>> 8895288015f78987fe1192eeaa34d5fe7903a71c
};
</script>
<style lang="scss" scoped>
</style>