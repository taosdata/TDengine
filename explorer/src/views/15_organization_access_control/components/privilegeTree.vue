<template>
  <el-tree accordion class="privilege-tree" expand-on-click-node :data="options" node-key="id">
    <el-tooltip slot-scope="{ node, data }" class="item" :content="node.label" placement="right">
      <div class="flexBetween">
        <el-checkbox
          style="margin-right: 5px"
          @change="change($event, data)"
          v-if="!data.children"
          class="left"
          :value="isChecked(data)"
        ></el-checkbox>
        <span class="center">{{ node.label }}</span>
      </div>
    </el-tooltip>
  </el-tree>
</template>

<script>
  import { getPrivilegeTypeMap } from "./privilege";
  export default {
    props: {
      value: {
        type: Object,
        default: () => ({ org: [], common: [], instance: [], db: [] }),
      },
    },
    components: {},
    data() {
      return {};
    },
    computed: {
      options() {
        const typeMap = getPrivilegeTypeMap();
        return Object.keys(typeMap).map(key => {
          return {
            id: key,
            label: this.$t(key),
            children: typeMap[key],
          };
        });
      },
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {
      change(status, data) {
        if (status) {
          this.value[data.type].push(data.id);
        } else {
          this.value[data.type].splice(this.value[data.type].indexOf(data.id), 1);
        }
      },
      isChecked(val) {
        return this.value[val.type].some(item => item === val.id);
      },
    },
  };
</script>

<style scoped lang="scss">
  .privilege-tree {
    max-height: 30vh;
    overflow-y: auto;
  }
</style>
