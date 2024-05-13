<template>
  <div class="flexStart">
    <el-select
      v-model="data[config.field]"
      :allow-create="true"
      style="flex: 1"
      class="mr20"
      :disabled="loading"
      :placeholder="config.placeholder"
      :multiple="config.multiple"
      clearable
      filterable
      @change="change"
    >
      <el-option
        v-for="item in bucketList"
        :key="item.value"
        v-bind="item"
      ></el-option>
    </el-select>
    <el-tooltip
      placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
    >
      <template slot="content">
        <span v-html="$t('communityTip')"></span>
      </template>
      <el-button
        :loading="loading"
        :disabled="loading || $COMMUNITY"
        type="primary"
        @click="search"
        >{{ $t('datasource.get' + (isInfluxdb ? 'schema' : 'metrics')) }}</el-button
      >
    </el-tooltip>
  </div>
</template>

<script>
import { getUaAndDaData } from '@/api/explorer/datain';
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
      bucketList: []
    };
  },
  computed: {
    measurementConfig() {
      return this.parentConfigList.find(item => item.field === 'measurements') ?? {};
    },
    isEdit() {
      return this.sourceParent.isEditable;
    },
    isInfluxdb() {
      return this.sourceParent.sourceForm.type === 'influxdb';
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
  watch: {},
  created() {},
  mounted() {
    if (this.isEdit) {
      this.search();
    }
  },
  methods: {
    search() {
      const errorMsg = [];
      const validFieldList = this.validFieldList.filter(item => document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`));
      this.sourceParent.$refs.form.validateField(validFieldList, valid => {
        errorMsg.push(valid);
        if (errorMsg.length == validFieldList.length && errorMsg.every(item => !item)) {
          let type = this.sourceParent.sourceForm.type
          const params = {
            from: type + getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition),
            categories: ['nodes'],
            pattern: 'api',
            offset: 0,
            limit: 10,
          };
          if (this.sourceParent.sourceForm.agent) {
            params["via"] = this.sourceParent.sourceForm.agent;
          }
          this.isInfluxdb ? this.searchBucket(params) : this.searchMetrics(params);
        } else {
          this.$nextTick(() => {
            document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
          });
        }
      });
      
    },
    searchBucket(params) {
      if (this.loading) return;
      this.loading = true;
      getUaAndDaData(params)
        .then(res => {
          if (!res[0] || !res?.[0]?.id) return (this.bucketList = []);
          const data = jsonToObj(res[0].id);
          this.bucketList = Object.keys(data).map(item => {
            return {
              label: item,
              value: item,
              chidlren: data[item].map(ite => ({ label: ite, value: ite }))
            };
          });
          if (!this.bucketList.some(item => item.value === this.data[this.config.field])) {
            this.data[this.config.field] = this.bucketList?.[0]?.value;
          }
          this.change(this.data[this.config.field]);
        })
        .catch(() => {
          this.bucketList = [];
        })
        .finally(() => {
          this.loading = false;
        });
    },
    searchMetrics(params) {
      if (this.loading) return;
      this.loading = true;
      getUaAndDaData(params)
        .then(res => {
          if (!res[0] || !res?.[0]?.id) return (this.bucketList = []);
          this.bucketList = jsonToObj(res[0].id).map(item => ({
            label: item?.id ?? item,
            value: item?.id ?? item
          }));
        })
        .catch(() => {
          this.bucketList = [];
        })
        .finally(() => {
          this.loading = false;
        });
    },
    change(val) {
      const measurementsOptions = this.bucketList.find(item => item.value === val)?.chidlren ?? [];
      this.measurementConfig.options = measurementsOptions;
      if (!measurementsOptions.some(item => item.value === this.data.measurements)) {
        this.data.measurements = '';
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
  }
};
</script>

<style scoped lang="scss"></style>
