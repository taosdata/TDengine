<template>
  <div>
    <el-form size="small" ref="Form" label-width="120px" label-position="left" :model="info">
      <el-form-item :label="$t('support.desc')" prop="content" required>
        <el-input v-model="info.content" type="textarea" placeholder="" rows="4"></el-input>
      </el-form-item>
      <el-form-item :label="$t('support.attachment')">
        <el-upload
          action=""
          ref="upload"
          v-model="info.file"
          :auto-upload="false"
          :on-remove="handleRemove"
          :on-preview="handlePreview"
          :on-change="handleChange"
          :file-list="fileList"
          accept=".png,.jpg,.jpeg,.txt,.zip"
          :limit="5"
        >
          <div class="flexCenter">
            <el-button slot="trigger" size="small" plain type="primary">{{ $t("support.selectFile") }}</el-button>
            <div slot="tip" v-html="$t('support.uploadTip')" style="line-height: 110%; margin-left: 10px; text-align: left"></div>
          </div>
        </el-upload>
      </el-form-item>
      <el-form-item label=" ">
        <el-button @click="createComment()" :disabled="requestIng" type="primary">{{ $t("create") }}</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
  import { createComment, uploadFile } from "@/api/gateway/support";
  export default {
    props: {
      typeList: {
        type: Array,
        default: () => [],
      },
    },
    data() {
      this.fileLimitSize = 10 * 1024 * 1024; //5M
      return {
        info: { content: "", ids: [] },
        fileUrl: process.env.VUE_APP_BASE_URL + "/files",
        fileList: [],
        requestIng: false,
      };
    },
    inject: ["detail"],
    methods: {
      beforeUpload() {},
      async handlePreview(file) {
        let url = window.URL.createObjectURL(file.raw);
        window.open(url);
      },
      handleRemove(_, fileList) {
        this.fileList = fileList;
      },
      handleChange(_, fileList) {
        this.fileList = fileList;
        this.checkFileSize();
      },
      async upload() {
        if (!this.checkFileSize()) return;
        if (!this.checkFileSize()) return;
        let uploadFileList = this.fileList.filter(item => !item.id);
        this.requestIng = true;
        if (uploadFileList.length) {
          let isSuccess = await Promise.allSettled(
            uploadFileList.map(item => {
              let formData = new FormData();
              formData.append("file", item.raw);
              return uploadFile(formData).then(res => {
                item.id = res.data.id;
                item.status = "success";
              });
            })
          )
            .then(result => {
              return result.every(item => item.status === "fulfilled");
            })
            .catch(() => false);
          if (!isSuccess) {
            this.requestIng = false;
            return;
          }
        }
        this.info.ids = this.fileList.map(item => item.id);
        this.info.ids = this.fileList.map(item => item.id);
        await createComment(this.info, this.$route.params.id)
          .then(() => {
            this.detail.getData();
            this.$emit("close");
            this.$message.success(this.$t("createSucc"));
            this.$refs.Form.resetFields();
            this.fileList = [];
            this.info = { content: "", ids: [] };
          })
          .catch(() => ({}));
        this.requestIng = false;
      },
      // 检查文件总大小
      checkFileSize() {
        let sum = this.fileList.reduce((pre, cur) => pre + cur.size, 0);
        if (sum < this.fileLimitSize) {
          return true;
        }
        this.$error(this.$t("support.fileSizeLarge"));
        return false;
      },

      createComment() {
        if (this.requestIng) return;
        this.$refs.Form.validate(async valid => {
          if (valid) {
            this.upload();
          } else {
            this.$error(this.$t("formatError"));
            return false;
          }
        });
      },
    },
  };
</script>

<style lang="scss"></style>
