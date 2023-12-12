<template>
  <el-upload
    class="upload-csv"
    ref="upload"
    :data="uploadData"
    accept=".csv"
    :on-remove="handleRemove"
    :on-preview="handlePreview"
    :action="uploadUrl"
    :multiple="false"
    :limit="1"
    :on-success="handleSuccess"
    :on-change="handleChange"
    :file-list="files"
    :auto-upload="true"
  >
    <el-button
      slot="trigger"
      size="mini"
      type="primary"
      :disabled="(!!files.length && !isEdit) || formDisabled"
      >{{ $t('support.selectFile') }}</el-button
    >
  </el-upload>
</template>

<script>
import { handleDownload } from '../utils';
export default {
  props: {
    config: {
      type: Object,
      default: () => ({})
    },
    value: {
      type: String,
      default: ''
    }
  },
  inject: ['sourceParent'],
  components: {},
  data() {
    return {
      files: []
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
    }
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {
    handleRemove(_, fileList) {
      this.files = fileList;
      this.update();
    },
    handleChange() {},
    handlePreview(file) {
      handleDownload(file.path, file.name);
    },
    handleSuccess(response, file, tmpFiles) {
      file.path = response[0];
      this.files = tmpFiles;
      this.update();
    },
    update() {
      this.$emit(
        'input',
        this.files
          .filter(item => item.path)
          .map(item => '@' + item.path)
          .join(',')
      );
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
</style>
