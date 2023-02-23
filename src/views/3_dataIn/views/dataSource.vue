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
      <el-table-column label="Source" prop="from"></el-table-column>
      <el-table-column label="Target" prop="to"></el-table-column>
      <el-table-column label="Created At" prop="created_at"></el-table-column>
      <el-table-column label="Finished At" prop="finished_at"></el-table-column>
      <el-table-column label="Status" prop="status"></el-table-column>
      <el-table-column label="Action" width="150" class="action">
        <template slot-scope="scope">
          <!-- <el-button
            type="primay"
            size="small"
            @click="checkMore(scope.row)"
            icon="el-icon-more"
          ></el-button> -->
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
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
      title="Add New Data Source"
      width="400px"
      :visible.sync="dialog"
    >
      <el-form
        :model="ruleForm"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        label-position="left"
        class="demo-ruleForm"
      >
        <el-form-item label="Source Type" prop="name" required>
          <el-select
            v-model="ruleForm.name"
            placeholder="Please Select Source Type"
          >
            <el-option
              :label="item.name"
              :value="item.id"
              v-for="item in sourceList"
              :key="item.id"
            ></el-option>
            <!-- <el-option label="OpenTSDB" value="opentsdb"></el-option>
            <el-option label="OPC" value="opc"></el-option>
            <el-option label="Kafka" value="kafka"></el-option> -->
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
        <el-col :span="5" :offset="6">
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
import { Message } from "element-ui";
import dbsource from "./datasource.json";
export default {
  name: "DataSource",
  props: {
    sourceList: {
      type: Array,
      default() {
        return [];
      },
    },
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.name) {
        return true;
      }
      if (!this.ruleForm.status) {
        return true;
      }
      return false;
    },
  },
  data() {
    return {
      dbsource: dbsource,
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      ruleForm: {
        name: "",
        status: "",
        time: "",
      },
      // rules:{
      //   name:[

      //       { required: true, message: 'Please select the source type', trigger: 'change' }

      //   ]
      // },
      topicList: [
        // {
        //   id: 1,
        //   data_source_name: "InfluxDB",
        //   data_source_type:'type123',
        //   data_source_target:'target123',
        //   status: "Pending",
        //   create_time: "2022-10-10 12:02:10",
        // },
        // {
        //   id: 2,
        //   data_source_name: "OpenTSDB",
        //   data_source_type:'type123',
        //   data_source_target:'target123',
        //   status: "Fullfiled",
        //   create_time: "2022-10-20 12:02:10",
        // },
        // {
        //   id: 3,
        //   data_source_name: "OPC",
        //   data_source_type:'type123',
        //   data_source_target:'target123',
        //   status: "Fullfiled",
        //   create_time: "2022-10-20 12:02:10",
        // },
        // {
        //   id: 4,
        //   data_source_name: "Kafka",
        //   data_source_type:'type123',
        //   data_source_target:'target123',
        //   status: "Fullfiled",
        //   create_time: "2022-10-20 12:02:10",
        // },
      ],
    };
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm(
        "Are you sure  to delete " + data.data_source_name + "?",
        "Warning",
        {
          confirmButtonText: "Ok",
          cancelButtonText: "Cancle",
          type: "warning",
        }
      ).then((res) => {
        fetch(`http://192.168.0.201:6050/tasks/${data.id}`, {
          method: "delete",
        })
          .then((res) => {
            if (res.status == 200) {
              Message({
                type: "success",
                message: "Deleted Successfully",
              });
              this.getList();
            }
          })
          .catch((err) => {
            err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      });
    },
    checkMore(data) {
      this.$router.push({
        path: `/dataIn/source/${data.data_source_name}`,
      });
    },
    handleAdd() {
      this.$parent.toggleComponent("ui", this.ruleForm.name);
    },
    async getList() {
      try {
        fetch("http://192.168.0.201:6050/tasks", {
          method: "get",
        })
          .then((res) => res.json())
          .then((result) => {
            this.topicList = result.map((item) => {
              if (item.status === "failed") {
                item["status"] = "(failed) " + " " + item.reason;
              }
              return item;
            });
          });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    start(data, index) {
      try {
        fetch(`http://192.168.0.201:6050/tasks/${data.id}/start`, {
          method: "post",
        }).then((res) => {
          if (res.status == 200) {
            this.getList();
          } else {
            Message({
              type: "error",
              message: "",
            });
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    stop(data, index) {
      try {
        fetch(`http://192.168.0.201:6050/tasks/${data.id}/stop`, {
          method: "post",
        }).then((res) => {
          if (res.status == 200) {
            this.getList();
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getList();
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