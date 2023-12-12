<template>
  <div label-width="0px">
    <el-radio-group
      v-if="config.radio != false"
      class="mb20"
      v-model="isAll"
      :disabled="formDisabled"
    >
      <el-radio label="file">{{ $t('uploadcsv') }}</el-radio>
      <el-radio label="*">{{ allCategoryText }}</el-radio>
    </el-radio-group>
    <section
      v-show="isAll != '*'"
      class="flexStart"
    >
      <uploadCsv
        v-model="value"
        :config="config"
      >
      </uploadCsv>

      <el-tooltip
        effect="light"
        :content="$t('downloadTemplateTip')"
      >
        <a
          v-if="config.templateUrl"
          class="ml20"
          :href="config.templateUrl"
          download
        >
          <i class="el-icon-download"></i>
          {{ $t('downloadTemplate') }}</a
        >
      </el-tooltip>
      <el-tooltip
        effect="light"
        :content="downloadPontTipText"
      >
        <a
          class="ml20"
          @click.prevent="downloadAllPointFile"
        >
          <i class="el-icon-download"></i>
          {{ downloadPointsText }}</a
        >
      </el-tooltip>
      <section
        v-if="isEdit"
        class="file-list"
      >
        <div
          v-for="file in oldFiles"
          :key="file.name"
          class="file-item"
          @click="handleDownload(file.path, file.name)"
        >
          <el-tooltip
            effect="light"
            :content="$t('dataIn.downloadCurrentFile')"
          >
            <p class="file-name">
              <i class="el-icon-download"></i>
              <span>{{ $t('dataIn.csvFileInUse') }}</span>
            </p>
          </el-tooltip>
        </div>
      </section>
    </section>
  </div>
</template>

<script>
import uploadCsv from './uploadCsv.vue';
import { downlaodAllNodes as downloadAllPointFile } from '@/api/explorer/datain';
import { getDsnData } from '../utils';
import { downloadFileBlob } from '@/utils/file';
import { handleDownload } from '../utils';

export default {
  props: {
    data: {
      type: Object,
      default: () => ({})
    },
    config: {
      type: Object,
      default: () => ({})
    }
  },
  inject: ['getCurrentDefinition', 'sourceParent'],
  components: { uploadCsv },
  data() {
    this.textMap = {};
    return {
      requestIng: false,
      fileValue: '',
      oldFiles: []
    };
  },
  computed: {
    formDisabled() {
      return this.sourceParent.formDisabled;
    },
    currentDefinition() {
      return this.getCurrentDefinition();
    },
    downloadPointsText() {
      const isPi = this.currentDefinition.id === 'pi';
      const piText = {
        point_file: 'allPoints',
        template_for_pi_point_file: 'afElementTemplate',
        template_for_af_element_file: 'afElementTemplate'
      }[this.config.field];
      if (piText == 'afElementTemplate') return this.$t('downloadAfElement');
      return this.$t('downloadPiPoint', [this.$t('dataIn.' + (isPi ? piText : 'allNodes'))]);
    },
    allCategoryText() {
      return this.$t(
        // 'dataIn.' +
          {
            point_file: 'allPoints',
            template_for_pi_point_file: 'allTemplate',
            template_for_af_element_file: 'allTemplate',
            csv_config_file: 'allNodes'
          }[this.config.field]
      );
    },
    downloadPontTipText() {
      const isPi = this.currentDefinition.id === 'pi';
      return this.$t(
        // 'dataIn.' +
          (isPi
            ? {
                point_file: 'downloadPiPointTip',
                template_for_pi_point_file: 'downloadAfElementTip',
                template_for_af_element_file: 'downloadAfElementTip'
              }[this.config.field]
            : 'allNodes')
      );
    },
    allData() {
      return this.sourceParent.sourceForm;
    },
    value: {
      get() {
        return this.data[this.config.field];
      },
      set(val) {
        this.data[this.config.field] = val;
      }
    },
    isAll: {
      get() {
        return this.value === '*' ? '*' : 'file';
      },
      set(val) {
        if (val == '*') {
          this.fileValue = this.value;
          this.value = '*';
        } else {
          this.value = this.fileValue;
        }
      }
    },
    isEdit() {
      return this.sourceParent.isEdit;
    },
    category() {
      return {
        point_file: 'PointList',
        template_for_pi_point_file: 'TemplateForPIPoint',
        template_for_af_element_file: 'TemplateForAFElement',
        csv_config_file: 'nodes'
      }[this.config.field];
    }
  },
  watch: {},
  created() {},
  mounted() {
    if (this.value != '*' && this.value && this.isEdit) {
      this.oldFiles = this.getFileList(this.value);
    }
  },
  methods: {
    downloadAllPointFile() {
      const url = getDsnData(this.allData.data, this.sourceParent.currentDefinition);
      console.log('url',url);
      if (!/:\/\/\w+?/.test(url)) return this.$message.error(this.$t('dataIn.noDsn'));
      if (this.requestIng) return;
      this.requestIng = true;
      const loading = this.$loading({
        lock: true,
        text: 'Loading',
        spinner: 'el-icon-loading',
        background: 'rgba(0, 0, 0, 0.7)'
      });
      downloadAllPointFile({
        from: url,
        categories: this.category,
        via: this.sourceParent.sourceForm.agent
      })
        .then(res => {
          downloadFileBlob(res, this.allCategoryText + '.csv');
        })
        .finally(() => {
          this.requestIng = false;
          loading.close();
        });
    },
    handleDownload,
    getFileList(data) {
      return data.split(',').map(item => {
        const name = item.slice(item.lastIndexOf('/') + 1);
        const path = item.slice(1);
        return {
          name,
          path
        };
      });
    }
  }
};
</script>

<style scoped lang="scss">
.file-list {
  margin-left: 20px;
  color: $color-primary;
  .file-item {
    display: flex;
    align-items: center;
    font-size: 14px;
    cursor: pointer;
    &:not-first-child {
      margin-top: 5px;
    }
    .file-name {
      flex: 1;
      @extend .nowrap;
    }
    .file-btn {
      font-size: 12px;
      display: none;
      padding-left: 20px;
      span {
        cursor: pointer;
        & + span {
          margin-left: 10px;
        }
      }
    }
    &:hover {
      color: $color-primary;
      background-color: #f5f7fa;
      .file-btn {
        display: flex;
      }
    }
  }
}
</style>
