<template>
  <div style="display: flex">
    <el-tooltip
      placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
    >
      <template slot="content">
        <span v-html="$t('communityTip')"></span>
      </template>
      <el-button
        v-if="isOpcDataset && !isOpcDsn"
        size="mini"
        plain
        type="primary"
        @click="handleBeforeUpload"
        :disabled="$COMMUNITY || disabled"
        >{{ $t('support.selectFile') }}</el-button
      >
    </el-tooltip>
    <el-tooltip
      placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
    >
      <template slot="content">
        <span v-html="$t('communityTip')"></span>
      </template>
    <el-upload
      class="upload-csv"
      ref="upload"
      :data="uploadData"
      :accept="accept"
      :on-remove="handleRemove"
      :on-preview="handlePreview"
      :action="uploadUrl"
      :multiple="false"
      :on-success="handleSuccess"
      :on-change="handleChange"
      :file-list="files"
      :auto-upload="true"
    >
      <el-button
        v-if="!isOpcDataset || isOpcDsn"
        slot="trigger"
        size="small"
        plain
        icon="el-icon-upload2"
        type="primary"
        ref="uploadButton"
        :disabled="$COMMUNITY || disabled"
      >{{ btnText || $t('support.selectFile') }}</el-button>
    </el-upload>
  </el-tooltip>
  </div>
</template>

<script>
import { handleDownload, getDsnData, getFieldClassMarkName } from '../utils';
import { validOpcFile } from '@/api/explorer/datain';
export default {
  props: {
    config: {
      type: Object,
      default: () => ({})
    },
    value: {
      type: String,
      default: ''
    },
    isOpcDataset: {
      type: Boolean
    }, 
    disabled: {
      type: Boolean,
      default: false
    },
    btnText: {
      type: String,
      default: ''
    }
  },
  inject: ['sourceParent'],
  components: {},
  data() {
    return {
      files: [],
      isOpcDsn: false, // 判断 opc 的 dsn 是否填了
      paramDsn: ''
    };
  },
  computed: {
    uploadData() {
      return { req_id: new Date().getTime() }
    },
    uploadUrl() {
      return process.env.VUE_APP_X_API + `/upload`;
    },
    isEdit() {
      return this.sourceParent.isEditable;
    },
    formDisabled() {
      return this.sourceParent.formDisabled;
    },
    accept() {
      return this.config.accept || ''
    },
    validFieldList() {
      const result = [];
      this.getValidFieldList(this.sourceParent.currentDefinition.config, result);
      return result;
    },
  },
  watch: {},
  created() {
    if(this.isEdit && this.value) {
      this.handleValidOpcFile()
    }
    // 在新增或者编辑时切换 tab 都能保持上传的文件列表
    this.handleFiles()
  },
  mounted() {
    this.$eventBus.$on("updatePIDefaultConfigFile", (defaultFile) => {
      this.files = [].concat({
        name: defaultFile?.substr(defaultFile.lastIndexOf("/") + 1),
        path: defaultFile,
        percentage: 100,
        raw: File,
        response: [].concat(this.value),
        size: 87,
        status: "success",
        uid: 1,
      });
      this.update();
    })
  },
  methods: {
    handleRemove(_, fileList) {
      this.files = fileList;
      this.update();
    },
    handleChange() {},
    handlePreview(file) {
      handleDownload(file.path, file.name);
    },
    async handleSuccess(response, file, tmpFiles) {
      file.path = response[0];
      this.files = [].concat(file);
      this.update();
      this.handleValidOpcFile()
    },
    async handleValidOpcFile() {
      if (this.isOpcDataset) {
        // csv 文件合法性检查
        const type = this.sourceParent.sourceForm.type
        const agent = this.sourceParent.sourceForm.agent
        const dsn = getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
        this.paramDsn = type === "tmq" ? dsn : type + dsn
        let result = await validOpcFile(this.paramDsn)
        // eslint-disable-next-line no-prototype-builtins
        if (result && result.hasOwnProperty('code')) {
          this.$error(result.message)
          let res = {
            valid: false,
            message: result.message
          }
          // 全局的参数用于提交的时候再次判断
          this.$store.commit('app/SET_VALDIT_OPC_FILE_RES',res)
        } else {
          this.$store.commit('app/SET_VALDIT_OPC_FILE_RES',result)
          this.$message.success(result.message)
        }
        this.isOpcDsn = false;
      }
    },
    handleBeforeUpload(event) {
      this.checkResult = this.$options.data().checkResult;
      const errorMsg = [];
      const validFieldList = this.validFieldList.filter(item => document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`));
      this.sourceParent.$refs.form.validateField(validFieldList, valid => {
        errorMsg.push(valid);
        if (errorMsg.length == validFieldList.length && errorMsg.every(item => !item)) {
          this.isOpcDsn = true;
          this.$nextTick(() => {
            this.$refs.uploadButton.$el.click();
            this.isOpcDsn = false;
          })
        } else {
          this.isOpcDsn = false;
          this.paramDsn = "";
          this.$nextTick(() => {
            document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
          });
        }
      });
    },
    getValidFieldList(data, result, parent = 'data') {
      for (const val of data) {
        if (val.field == 'checkConnectivity') break;
        if (val.children) {
          this.getValidFieldList(val.children, result, parent + '.' + val.field);
        } else {
          if (val.required) {
            result.push(parent + '.' + val.field);
          }
        }
      }
    },
    update() {
      this.$emit(
        'input',
        this.files
          .filter(item => item.path)
          .map(item => '@' + item.path)
          .join(',')
      );
    },
    handleFiles() {
      if (this.value && this.value != "*") {
        this.files = [].concat({
          name: this.value.substr(this.value.lastIndexOf("/") + 1),
          path: this.value.startsWith("@") ? this.value.substr(1) : this.value,
          percentage: 100,
          raw: File,
          response: [].concat(this.value),
          size: 87,
          status: "success",
          uid: 1,
        });
      }
    }
  }
};
</script>

<style scoped lang="scss">
.upload-csv {
  display: flex;
  align-items: center;
  &:deep(.el-upload-list__item) {
    margin-top: 0;
    margin-left: 1rem;
  }
}
::v-deep .el-upload-list__item.is-success.focusing .el-icon-close-tip {
  display: none !important;
}
</style>
