<template>
  <div class="flexStart">
    <el-input
      v-if="isEdit && !isCopy"
      v-model="data[config.field]"
      style="flex: 1"
      :placeholder="config.placeholder"
      :disabled="isEdit"
      >
    </el-input>
    <template v-else>
      <span>taosx</span>
      <el-input
        v-model="data[config.field]"
        style="flex: 1"
        class="mr20 ml15"
        :placeholder="config.placeholder"
      >
      </el-input>
     
      <el-tooltip
        placement="top" effect="light" :open-delay="0"
      >
        <template slot="content">
          <span v-html="$t('dataIn.taskIdTip',[config.label])"></span>
        </template>
        <el-switch
          v-model="data[switchField]"
          :loading="loading"
          :disabled="loading"
          type="primary"
          @click="search"
          >{{ $t('datasource.transformer.preview') }}</el-switch
        >
      </el-tooltip>  
    </template>
  </div>
</template>

<script>
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
    },
    parentConfigList: {
      type: Array,
      default: () => []
    }
  },
  mixins: [mixinItem],
  inject: ['sourceParent'],
  components: {},
  data() {
    return {};
  },
  computed: {
    isEdit() {
      return this.sourceParent.isEditable;
    },
    isCopy() {
      return this.sourceParent.isCopyable;
    },
    switchField() {
      return `${this.config.field.startsWith('group') ? this.config.field + '_id' : this.config.field}_with_task_id`
    },
    taskId() {
      return this.sourceParent.editId
    }
  },
  created() {},
  mounted() {
    if (this.isEdit) {
      // 兼容历史任务的 group 回显任务 id
      this.data['group'] = this.data['group'] || this.taskId;
    }
    if (this.isCopy) {
      // 复制时置空group/client_id
      this.data[this.config.field] = ''
    }
  },
  methods: {}
};
</script>

<style scoped lang="scss"></style>
