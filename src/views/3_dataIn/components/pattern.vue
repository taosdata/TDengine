<template>
  <div class="flexStart">
    <el-input
      v-model="data[config.field]"
      style="flex: 1"
      class="mr20"
      :placeholder="config.placeholder"
    >
    </el-input>
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
        >{{ $t('datasource.transformer.preview') }}</el-button
      >
    </el-tooltip>         
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
  },
  created() {},
  mounted() {
    // 编辑时自动展示 node table
    // if (this.isEdit) {
    //   this.search();
    // }
  },
  methods: {}
};
</script>

<style scoped lang="scss"></style>
