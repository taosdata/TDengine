<template>
  <div class="flexStart">
    <el-input
      v-model="data[config.field]"
      style="flex: 1"
      class="mr20"
      :placeholder="config.placeholder"
    >
    </el-input>
    <el-button
      :loading="loading"
      :disabled="loading"
      type="primary"
      @click="search"
      >{{ $t('datasource.transformer.preview') }}</el-button
    >
  </div>
</template>

<script>
import { getUaAndDaData, getTicket, checkReadyFile, getDatasets } from '@/api/explorer/datain';
import { getDsnData, optionsField, getFieldClassMarkName } from '../utils';
import { jsonToObj } from '@/utils';
export default {
  props: {
    data: {
      type: Object,
      default: () => ({})
    },
    config: {
      type: Object,
      default: () => ({})
    },
    parentConfigList: {
      type: Array,
      default: () => []
    }
  },
  inject: ['sourceParent'],
  components: {},
  data() {
    return {
      loading: false,
      ticket: "",
      page: 0,
      pageSize: 1000,
      complete: false,
      category: 'PointList'
    };
  },
  computed: {
    isEdit() {
      return this.sourceParent.isEditable;
    },
    btnDisabled() {
      const optionData = this.sourceParent.sourceForm.data[optionsField];
      return !optionData?.host || !optionData?.port || !optionData?.protocol;
    },
    validFieldList() {
      const result = [];
      this.getValidFieldList(this.sourceParent.currentDefinition.config, result);
      return result;
    }
  },
  watch: {
    "$store.state.app.complete"(val) {
      if (val) {
        this.timer && clearInterval(this.timer)
        this.loading = false
      }
    }
  },
  created() {},
  mounted() {
    // 编辑时自动展示 node table
    // if (this.isEdit) {
    //   this.search();
    // }
  },
  methods: {
    search() {
      const errorMsg = [];
      const validFieldList = this.validFieldList.filter(item => document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`));
      this.sourceParent.$refs.form.validateField(validFieldList, valid => {
        errorMsg.push(valid);
        if (errorMsg.length == validFieldList.length && errorMsg.every(item => !item)) {
          let type = this.sourceParent.sourceForm.type
          let form = type + getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
          let via = this.sourceParent.sourceForm.agent;
         
          this.searchDatasets(form, via);
        } else {
          this.$nextTick(() => {
            document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
          });
        }
      });
      
    },
 
    async searchDatasets(from, via) {
      if (this.loading) return;
      try {
        this.loading = true;
        let result = await getTicket(from, via, this.category)
        this.ticket = result.ticket
        this.$store.commit("app/SET_TICKET",this.ticket);
  
        this.timer = setInterval(async () => {
          let { complete } = await checkReadyFile(result.ticket)
          this.complete = complete
          this.$store.commit("app/SET_COMPLETE",complete)
        }, 2000);
      } catch (error) {
        this.timer && clearInterval(this.timer)
      }
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
  },
  beforeDestroy() {
    this.timer && clearInterval(this.timer)
  }
};
</script>

<style scoped lang="scss"></style>
