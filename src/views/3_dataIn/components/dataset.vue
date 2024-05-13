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
      class="flexStart mb20"
    >
      <uploadCsv
        v-model="value"
        :config="config"
        :isOpcDataset="isOpc"
      >
      </uploadCsv>

      <el-tooltip
        v-if="isOpc"
        effect="light"
        :content="$t('downloadTemplateTip')">
        <a
          v-if="config.templateUrl"
          class="ml20"
          @click="handleDownEmptyTemplate"
        >
          <i class="el-icon-download"></i>
          {{ $t('downloadTemplate') }}</a
        >
      </el-tooltip>
      <el-tooltip
        v-else
        effect="light"
        :content="$t('downloadTemplateTip')">
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
        class="opc_download_point"
        effect="light"
        :content="downloadPontTipText"
        v-if="isOpc">
        <a
          class="ml20"
          @click.prevent="openDialog"
        >
          <i class="el-icon-download"></i>
          {{ downloadPointsText }} 
          <div class="csv_progress">
            <el-progress v-if="progressVisble" :percentage="percentage" :format="format"/>
          </div>
          </a
        >
      </el-tooltip>
      <el-tooltip
        effect="light"
        :content="downloadPontTipText"
        v-else>
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
        class="file-list">
        <div
          v-for="file in oldFiles"
          :key="file.name"
          class="file-item"
          @click="handleDownload(file.path, file.name)"
        >
          <el-tooltip
            effect="light"
            :content="$t('downloadCSVInUseTip')"
          >
            <p class="file-name">
              <i class="el-icon-download"></i>
              <span>{{ $t('downloadCSVInUse') }}</span>
            </p>
          </el-tooltip>
        </div>
      </section>
      <el-button
        v-if="value"
        :loading="loading"
        :disabled="loading"
        type="primary"
        size="mini"
        class="ml15"
        @click="search"
        >{{ $t('datasource.transformer.preview') }}</el-button
      >
    </section>
    <el-dialog
      :title="$t('dataIn.filterPointTitle')"
      :visible.sync="dialogVisible"
      :close-on-click-modal="false"
      width="500px">
      <div slot="title">
        <div class="el-dialog_cus_itle">{{ $t('dataIn.filterPointTitle') }}</div>
        <DocsContent
          :content="$t('dataIn.filterPoinDesc')"
        />
      </div>
      <div>
        <el-form 
        size="small" 
        :model="info" 
        ref="conditionForm"
        label-width="150px"
        label-position="left">
          <el-form-item
            :label="$t('dataIn.rootNode')"
            prop="root"
          >
            <el-input style="width: 300px" v-model="info.root" :placeholder="$t('dataIn.rootNodePlaceholder.' + sourceParent.sourceForm.type)"></el-input>
          </el-form-item>
          <el-form-item
            :label="$t('dataIn.namespace')"
            prop="namespaces"
            v-if="isOpcUa"
          >
            <el-select style="width: 300px" v-model="info.namespaces" :multiple="true" :placeholder="$t('dataIn.namespacePlaceholder')">
              <el-option
                v-for="item in namespaceList"
                :key="item.label"
                :value="item.value"
                :label="item.label"
              ></el-option>
            </el-select>
          </el-form-item><el-form-item
            :label="$t('dataIn.pointRegexp')"
            prop="pattern"
          >
            <el-input style="width: 300px" v-model="info.pattern" :placeholder="$t('dataIn.pointRegexpPlaceholder.' + sourceParent.sourceForm.type)"></el-input>
          </el-form-item>
        </el-form>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogVisible = false">{{ $t('cancel')}}</el-button>
        <el-button type="primary" @click="submit" :loading="requestIng">{{ $t('confirm')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import uploadCsv from './uploadCsv.vue';
import { downlaodAllNodes as downloadAllPointFile, downlaodOpcPointFile, getTicket, checkReadyFile, getCsvEmptyTemplate } from '@/api/explorer/datain';
import { getDsnData } from '../utils';
import { downloadFileBlob } from '@/utils/file';
import { handleDownload } from '../utils';
import DocsContent from '@/views/support/components/editorContentDisplay.vue';
import mixinItem from '../mixins/opcPreviewPoint.js';

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
  mixins: [mixinItem],
  inject: ['getCurrentDefinition', 'sourceParent'],
  components: { uploadCsv, DocsContent },
  data() {
    this.textMap = {};
    return {
      requestIng: false,
      fileValue: '',
      oldFiles: [],
      dialogVisible: false,
      progressVisble: false,
      info: {

      },
      ticket: '',
      percentage: 5,
      completed: false,
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
      return this.$t('downloadPiPoint', [this.$t('dataIn.' + (isPi ? piText : 'downloadnodestip'))]);
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
            : 'dataIn.downloadnodestip')
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
      return this.sourceParent.isEditable;
    },
    category() {
      return {
        point_file: 'PointList',
        template_for_pi_point_file: 'TemplateForPIPoint',
        template_for_af_element_file: 'TemplateForAFElement',
        csv_config_file: 'nodes'
      }[this.config.field];
    },
    isOpc() {
      return ["opcua","opcda"].includes(this.sourceParent.sourceForm.type)
    },
    isOpcUa() {
      return ["opcua"].includes(this.sourceParent.sourceForm.type)
    },
    namespaceList() {
      const { namespaces = [] } = this.$store.state.app.connectivityCheckResult
      let list = []
      namespaces.map((item,index) => {
        if (index > 0) {
          list.push({ label: item, value: index}) 
        }
      })
      return list
    }
  },
  watch: {
    completed(val) {
      if (val) {
        this.timer && clearInterval(this.timer)
        this.percentage = 100
        // 调用下载接口
        this.downloadFile()
      }
    }
  },
  created() {},
  mounted() {
    if (this.value != '*' && this.value && this.isEdit) {
      this.oldFiles = this.getFileList(this.value);
    }
  },
  methods: {
    downloadAllPointFile() {
      let type = this.sourceParent.sourceForm.type
      let via = this.sourceParent.sourceForm.agent
      const url = type + getDsnData(this.allData.data, this.sourceParent.currentDefinition);
      if (!/:\/\/\w+?/.test(url)) return this.$error(this.$t('dataIn.noDsn'));
      if (this.requestIng) return;
      this.requestIng = true;
      let from = url + `&categories=${this.category}`;
      downloadAllPointFile(from, via)
        .then(res => {
          if(res && res.code) {
            return this.$error(res.message)
          }
          downloadFileBlob(res, this.allCategoryText + '.csv');
        })
        .finally(() => {
          this.requestIng = false;
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
    },
    openDialog() {
      this.dialogVisible = true
    },
    async submit() {
      let type = this.sourceParent.sourceForm.type
      let via = this.sourceParent.sourceForm.agent
      const url = type + getDsnData(this.allData.data, this.sourceParent.currentDefinition);
      if (!/:\/\/\w+?/.test(url)) return this.$error(this.$t('dataIn.noDsn'));
      if (this.requestIng) return;
      try {
        this.requestIng = true;
        let  filterParm = ''
        Object.keys(this.info).map(key => {
          if (this.info[key]) {
            filterParm += '&' + [key] + '=' + this.info[key] 
          }
        })
  
        let from = filterParm ? url + filterParm : url
        
        this.progressVisble = true
        let result = await getTicket(from, via, this.category)
        this.ticket = result.ticket
  
        this.timer = setInterval(async () => {
          let { complete } = await checkReadyFile(result.ticket)
          this.completed = complete
          const randomNum = Math.floor(Math.random() * 4);

          if (!complete) {
            this.percentage = this.percentage < 95 ? this.percentage + randomNum : 99;
          }
        }, 2000);
        this.dialogVisible = false  
      } catch (error) {
        this.timer && clearInterval(this.timer)
      }
    },
    // 下载 OPC csv
    async downloadFile() {
      const res = await downlaodOpcPointFile(this.ticket)
      if (res && res.code) {
        return this.$error(res.message)
      }
      downloadFileBlob(res, this.allCategoryText + '.csv');
      this.completed = false;
      this.requestIng = false
      setTimeout(() => {
        this.progressVisble = false;
        this.percentage = 5;
      },500)
    },
    format(percentage) {
      // return percentage === 100 ? 'CSV 文件准备中' : `${percentage}%`;
      return `${percentage}%`;
    },
    // 下载 CSV 空模版
    async handleDownEmptyTemplate() {
      console.log('hshshshhs-empty');
      let res = await getCsvEmptyTemplate(this.sourceParent.sourceForm.type)
      downloadFileBlob(res, this.$t('downloadTemplate') + '.csv');
    }

  },
  beforeDestroy() {
    this.timer && clearInterval(this.timer)
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
      & > i {
        margin-right: 3px;
      }
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
.opc_download_point {
  position: relative;
}
.csv_progress {
  position: absolute;
  width: 150px;
  // left: 18px;
}
.el-dialog_cus_itle {
  line-height: 26px;
  font-weight: 500;
  font-size: 20px;
  color: #4d6992;
}
</style>
