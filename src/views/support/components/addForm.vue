<template>
  <div>
    <el-form size="small" ref="Form" label-width="120px" label-position="left" :model="info">
      <el-form-item :label="$t('support.title')" prop="title" required>
        <el-input v-model="info.title" :placeholder="$t('support.titleTip')"></el-input>
      </el-form-item>
      <el-form-item :label="$t('support.belong')" prop="type" required>
        <el-select v-model="info.type" placeholder="">
          <el-option v-for="item in typeList" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('support.desc')" prop="description">
        <mavonEditor :language="language" :toolbars="toolbars" v-model="info.description" :placeholder="$t('support.descTip')"></mavonEditor>
      </el-form-item>
      <el-form-item :label="$t('support.attachment')">
        <el-upload
          action=""
          ref="upload"
          multiple
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
      <el-form-item v-show="info.subscribe" :label="$t('email')" prop="email" :rules="emailRule">
        <el-input v-model="info.email" :placeholder="$t('support.emailTip')"></el-input>
      </el-form-item>
      <el-form-item>
        <el-checkbox v-model="info.subscribe">{{ $t("support.likeState") }}</el-checkbox>
      </el-form-item>
      <el-form-item label=" ">
        <el-button @click="createIssue()" :disabled="requestIng" type="primary">{{ $t("submit") }}</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
  import { createNewIssueReq, uploadFile } from "@/api/gateway/support";
  import { validEmail } from "@/utils/validate";
  import { mavonEditor } from "mavon-editor";
  import "mavon-editor/dist/css/index.css";
  export default {
    components: { mavonEditor },
    data() {
      this.toolbars = {
        bold: true, // 粗体
        italic: true, // 斜体
        header: true, // 标题
        underline: true, // 下划线
        strikethrough: true, // 中划线
        mark: true, // 标记
        superscript: true, // 上角标
        subscript: true, // 下角标
        quote: true, // 引用
        ol: true, // 有序列表
        ul: true, // 无序列表
        link: true, // 链接
        code: true, // code
        table: true, // 表格
        fullscreen: true, // 全屏编辑
        readmodel: true, // 沉浸式阅读
        htmlcode: true, // 展示html源码
        help: true, // 帮助
        /* 1.3.5 */
        undo: true, // 上一步
        redo: true, // 下一步
        trash: true, // 清空
        /* 1.4.2 */
        navigation: true, // 导航目录
        /* 2.1.8 */
        alignleft: true, // 左对齐
        aligncenter: true, // 居中
        alignright: true, // 右对齐
        /* 2.2.1 */
        subfield: true, // 单双栏模式
        preview: true, // 预览
      };
      this.validateEmail = (_, value, callback) => {
        if (value && !validEmail(value)) {
          callback(new Error(this.$t("emailError")));
        } else {
          callback();
        }
      };
      this.fileLimitSize = 10 * 1024 * 1024; //5M
      return {
        info: {},
        fileList: [],
        requestIng: false,
      };
    },
    computed: {
      typeList() {
        return this.$store.state.issues.issuetype_list;
      },
      emailRule() {
        return [{ validator: this.validateEmail, trigger: "blur" }];
      },
      language() {
        return this.$store.state.language;
      },
    },
    created() {
      this.info = {
        title: "",
        description: "",
        ids: [],
        email: this.$store.state.app.userInfo.email,
        type: this.typeList[0]?.value,
        subscribe: true,
      };
    },
    methods: {
      handleRemove(_, fileList) {
        this.fileList = fileList;
      },
      async handlePreview(file) {
        let url = window.URL.createObjectURL(file.raw);
        window.open(url);
      },

      handleChange(_, fileList) {
        this.fileList = fileList;
        this.checkFileSize();
      },
      async upload() {
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
        await createNewIssueReq(this.info)
          .then(() => {
            this.$emit("close");
            this.$message.success(this.$t("createSucc"));
            this.$refs.Form.resetFields();
            this.fileList = [];
            this.info = {
              title: "",
              description: "",
              ids: [],
              email: this.$store.state.auth.userInfo.email,
              type: this.typeList[0]?.value,
              subscribe: true,
            };
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

      createIssue() {
        if (this.requestIng) return;
        this.$refs.Form.validate(valid => {
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
