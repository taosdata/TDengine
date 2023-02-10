<template>
  <div class="data-source">
    <div class="flexEnd">
      <el-button
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
        :label="$t('topic.data_source_type')"
        prop="data_source_type"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.data_source_target')"
        prop="data_source_target"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.status')"
        prop="status"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.create_time')"
        prop="create_time"
      ></el-table-column>

      <el-table-column label="Action" width="100" class="action">
        <template slot-scope="scope">
          <el-button
            type="primay"
            size="small"
            @click="checkMore(scope.row)"
            icon="el-icon-more"
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
      :title="$t('topic.addsource')"
      width="400px"
      :visible.sync="dialog"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        :label-position="left"
        class="demo-ruleForm"
      >
        <el-form-item label="Source Type" prop="name" required>
          <el-select
            v-model="ruleForm.name"
            placeholder="Please Select Source Type"
          >
            <el-option label="InfluxDB" value="influxdb"></el-option>
            <el-option label="OpenTSDB" value="opentsdb"></el-option>
            <el-option label="OPC" value="opc"></el-option>
            <el-option label="Kafka" value="kafka"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Source Name" prop="status" required>
         <el-input v-model="ruleForm.status"></el-input>
        </el-form-item>
        <!-- <el-form-item label="Created Time" required>
          <el-form-item prop="time">
            <el-date-picker
              v-model="ruleForm.time"
              type="datetime"
              placeholder="Please Select Date And Time"
            >
            </el-date-picker>
          </el-form-item>
        </el-form-item> -->
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
            :disabled="confirmStatus"
            @click="handleAdd"
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
  computed:{
    confirmStatus(){
        if(!this.ruleForm.name){
            return true
        }
        if(!this.ruleForm.status){
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
        name: "",
        status: "",
        time: "",
      },
      topicList: [
        {
          id: 1,
          data_source_name: "InfluxDB",
          data_source_type:'type123',
          data_source_target:'target123',
          status: "Pending",
          create_time: "2022-10-10 12:02:10",
        },
        {
          id: 2,
          data_source_name: "OpenTSDB",
          data_source_type:'type123',
          data_source_target:'target123',
          status: "Fullfiled",
          create_time: "2022-10-20 12:02:10",
        },
        {
          id: 3,
          data_source_name: "OPC",
          data_source_type:'type123',
          data_source_target:'target123',
          status: "Fullfiled",
          create_time: "2022-10-20 12:02:10",
        },
        {
          id: 4,
          data_source_name: "Kafka",
          data_source_type:'type123',
          data_source_target:'target123',
          status: "Fullfiled",
          create_time: "2022-10-20 12:02:10",
        },
      ],
    };
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.data_source_name + '?', "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning",
      });
    },
    checkMore(data) {
      this.$router.push({
        path: `/dataIn/source/${data.data_source_name}`,
      });
    },
    handleAdd(){}
  },
};
</script>
<style lang="scss" scoped>
::v-deep.el-form-item__label {
  white-space: nowrap !important;
  margin-right: 100px;
}
.el-form-item {
  display: flex;
}
::v-deep.el-form-item--mini .el-form-item__content {
  margin-left: 0px !important;
}
::v-deep.el-input--mini .el-input__inner,
::v-deep.el-input.el-input--mini.el-input--suffix {
  width: 172px !important;
}
::v-deep.input.el-input__inner {
  width: 172px !important;
}
</style>