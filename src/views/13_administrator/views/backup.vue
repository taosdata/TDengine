<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button plain @click="refresh" size="small" icon="el-icon-refresh">{{
        $t("refresh")
      }}</el-button>
      <el-button plain @click="add" size="small" icon="el-icon-plus"
        >Create New Backup</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column label="ID" width="100" prop="id"></el-table-column>
      <el-table-column label="Databse" prop="database"></el-table-column>
      <el-table-column label="Create Time" prop="created_at"></el-table-column>
      <el-table-column
        label="Last Backup Status"
        prop="status"
      ></el-table-column>

      <el-table-column label="Operation" width="150">
        <template slot-scope="scope">
          <el-switch
            :value="scope.row.status.toLowerCase() == 'running'"
            active-color="rgb(66, 89, 206)"
            inactive-color="#dcdfe6"
            @change="switchOperation($event, scope.row)"
          >
          </el-switch>
          <el-button
            plain
            size="small"
            @click="edit(scope.row, scope.$index)"
            icon="el-icon-edit"
          ></el-button>
          <!-- <el-button
            plain
            size="small"
            @click="start(scope.row, scope.$index)"
            icon="el-icon-qidong"
          ></el-button> -->
          <!-- <el-button
            plain
            size="small"
            @click="stop(scope.row, scope.$index)"
            icon="el-icon-tingzhi"
          ></el-button> -->
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
      :title="dialogTitle"
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
        <el-form-item prop="cycle" required label="Backup Cycle">
          <el-select v-model="ruleForm.cycle" placeholder="Please select">
            <el-option
              v-for="c in cycleList"
              :key="c.value"
              :label="c.label"
              :value="c.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Database" prop="db" required v-if="!isEditDialog">
          <el-select v-model="ruleForm.db" placeholder="Please select">
            <el-option
              v-for="db in dblist"
              :key="db['node-key']"
              :label="db.name"
              :value="db.name"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item
          label="Directory"
          prop="directory"
          required
          v-if="!isEditDialog"
        >
          <el-input v-model.trim="ruleForm.directory"></el-input>
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
            @click="submit"
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
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
export default {
  data() {
    return {
      dblist: [],
      isEditDialog: false,
      dialogTitle: "Create New Backup",
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      operateStatus: true,
      currentRow: null,
      ruleForm: {
        cycle: "",
        db: "",
        directory: "",
      },
      cycleList: [
        {
          label: "Everyday",
          value: "schedule:@daily",
        },
        {
          label: "Every 7 days",
          value: "schedule:@weekly",
        },
        {
          label: "Every 30 days",
          value: "schedule:@monthly",
        },
      ],
      rules: {
        cylce: [
          {
            message: "Please select backup cycle",
            trigger: "change",
          },
        ],
        db: [
          {
            message: "Please select the database",
            trigger: "change",
          },
        ],
        directory: [
          {
            message: "Please enter the directory",
            trigger: "blur",
          },
        ],
      },
      topicList: [],
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.cycle) {
        return true;
      }

      if (!this.ruleForm.db) {
        return true;
      }
      if (!this.ruleForm.directory) {
        return true;
      }

      return false;
    },
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm(
        "Are you sure  to delete " + data.id + " backup task?",
        "Warning",
        {
          confirmButtonText: "Ok",
          cancelButtonText: "Cancle",
          type: "warning",
        }
      ).then(() => {
        fetch(`http://192.168.0.201:6050/tasks/${data.id}`, {
          method: "delete",
        }).then((res) => {
          if (res.status == 200) {
            Message({
              type: "success",
              message: "Deleted Successfully",
            });
            this.getBackData();
          }
        });
      });
    },
    add() {
      this.dialogTitle = "Create New Backup";
      this.isEditDialog = false;
      this.dialog = true;
      this.ruleForm.db = "";
      this.ruleForm.directory = "";
    },
    refresh() {
      this.getBackData();
    },
    edit(data) {
      this.dialogTitle = "Change Backup";
      this.isEditDialog = true;
      this.dialog = true;
      this.ruleForm.db = data.database;
      this.ruleForm.directory = data.to;
      this.currentRow = data;
    },
    async start(val, data) {
      try {
        await fetch(`http://192.168.0.201:6050/tasks/${data.id}/start`, {
          method: "post",
        }).then((res) => {
          if (res.status == 200) {
            Message.success("Operation Successfully Completed!");
            this.getBackData();
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    stop(val, data) {
      try {
        fetch(`http://192.168.0.201:6050/tasks/${data.id}/stop`, {
          method: "post",
        }).then((res) => {
          if (res.status == 200) {
            Message.success("Operation Successfully Completed!");
            this.getBackData();
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    //切换状态
    switchOperation(val, data) {
      if (val) {
        this.start(val, data);
      } else {
        this.stop(val, data);
      }
    },
    submit() {
      if (this.isEditDialog) {
        //调用编辑接口
        this.editBakcup(this.currentRow.id);
      } else {
        this.addBackup();
      }
    },
    async editBakcup(id) {
      //哪一项修改传参只传哪一项
      try {
        await fetch(`http://192.168.0.201:6050/tasks/${id}`, {
          method: "put",
          body: JSON.stringify({
            trigger: this.ruleForm.cycle,
          }),
        }).then((res) => {
          console.log(res, "edit");
          if(res.status==200){
            this.getBackData()
          }else{
            Message.error(res.statusText)
          }
          this.dialog=false
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    async addBackup() {
      try {
        await fetch("http://192.168.0.201:6050/tasks", {
          method: "post",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            name: "bakcup",
            labels: [
              "type::backup",
              `cluster-id::${localStorage.getItem("local_clusterID")}`,
            ],
            trigger: this.ruleForm.cycle,
            to: `local:${this.ruleForm.directory}`,
            from: `tmq+${localStorage.getItem("base_url")}/${this.ruleForm.db}`,
          }),
        }).then((res) => {
          if (res.ok || res.status == 201) {
            Message.success("Created Successfully!");
            this.getBackData();
            this.dialog = false;
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    async getBackData() {
      try {
        let id = localStorage.getItem("local_clusterID");
        fetch(
          `http://192.168.0.201:6050/tasks?labels=type::backup,cluster-id::${id}`,
          {
            method: "get",
          }
        )
          .then((res) => res.json())
          .then((result) => {
            this.topicList = result.map((item) => {
              item["database"] = item.from.split("/").at(-1);

              return item;
            });
          });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getDatabases();
    this.getBackData();
  },
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}
.el-switch {
  margin-right: 10px;
}
</style>