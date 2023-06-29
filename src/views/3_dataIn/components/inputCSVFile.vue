<template>
  <div class="flexBetween">
    <el-form
      :disabled="requestIng"
      ref="inputCsvForm"
      :model="inputCsvForm"
      label-position="left"
      label-width="120px"
    >
      <el-form-item :label="$t('data.dbName')" prop="dbName" required>
        <el-select filterable placeholder="" v-model="inputCsvForm.dbName">
          <el-option
            v-for="item in dbList"
            :key="item.name"
            :value="item.name"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('data.tableName')" prop="tbName" required>
        <el-select
          placeholder=""
          v-model="inputCsvForm.tbName"
          :disabled="!inputCsvForm.dbName"
          :default-first-option="true"
          filterable
          :remote-method="remoteMethod"
          :loading="requestIng"
          @focus="remoteMethod(inputCsvForm.tbName)"
          remote
        >
          <el-option
            v-for="item in tableList"
            :key="item.table_name"
            :value="item.table_name"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="CSV" prop="data" required>
        <el-upload
          ref="uploadForm"
          class="upload-demo"
          accept=".csv"
          :file-list="fileList"
          :auto-upload="false"
          :multiple="false"
          drag
          action=""
          :limit="1"
          :on-change="handleFileChange"
          :on-remove="handleFileChange"
        >
          <i class="el-icon-upload"></i>
          <div class="el-upload__text" v-html="$t('dataIn.uploadText')"></div>
          <div class="el-upload__tip" v-html="$t('dataIn.uploadTip')"></div>
        </el-upload>
      </el-form-item>
      <el-form-item>
        <el-button
          class="w100"
          type="primary"
          :loading="requestIng"
          :disabled="submitDisabled"
          @click="submitForm"
          >{{ $t("submit") }}</el-button
        >
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import { uploadCsv } from "@/api/gateway/app";
import { getDBListReq } from "@/api/gateway/data/dbs";
import { searchTable } from "@/api/gateway/data/tables";
import { Message } from 'element-ui';
export default {
  data() {
    return {
      inputCsvForm: {
        dbName: "",
        tbName: "",
        data: "",
        appId: this.$store.getters.appId,
      },
      fileList: [],
      requestIng: false,
      dbList: [],
      tableList: [],
    };
  },
  computed: {
    submitDisabled() {
      return (
        this.requestIng ||
        !this.inputCsvForm.dbName ||
        !this.inputCsvForm.tbName ||
        !this.inputCsvForm.data
      );
    },
  },
  created() {
    this.getDBList();
  },
  methods: {
    submitForm() {
      if (this.requestIng) return;
      try {
        this.$refs.inputCsvForm.validate((valid) => {
          if (valid) {
            this.requestIng = true;
            const loading = this.$loading({
              lock: true,
              text: "Loading",
              spinner: "el-icon-loading",
              background: "rgba(0, 0, 0, 0.7)",
            });

            uploadCsv(this.inputCsvForm)
              .then((res) => {
                this.$refs.inputCsvForm.resetFields();
                this.fileList = [];
                this.$message.success(this.$t("dataIn.uploadSuccess"));
              })
              .finally(() => {
                loading.close();
                this.requestIng = false;
              }).catch(err=>{
                err&&Message.error(err.desc || err)
              });
          } else {
            return false;
          }
        });
      } catch (error) {
        console.log(error,'csvcuowu');
      }
    },
    handleFileChange(file) {
      this.inputCsvForm.data = file.raw;
    },
    remoteMethod(query) {
      if (this.requestIng) return;
      this.requestIng = false;
      searchTable(query, this.inputCsvForm.dbName)
        .then((data) => {
          this.tableList = data;
        })
        .catch((err) => {
          this.tableList = [];
          this.$message.error(err.desc);
        })
        .finally(() => {
          this.requestIng = false;
        });
    },
    getDBList() {
      getDBListReq().then((data) => {
        this.dbList = data;
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.flexBetween {
  margin-top: 40px;
  padding-left: 20px;
}
.upload-demo {
  width: 360px;
}
</style>
