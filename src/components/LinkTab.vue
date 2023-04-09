<template>
  <el-tabs v-model="activeName" @tab-click="tabClick">
    <el-tab-pane v-for="item in tabs" :key="item.name" v-bind="item"></el-tab-pane>
  </el-tabs>
</template>

<script>
  export default {
    props: {
      // 传入的数据
      tabs: {
        type: Array,
        default: () => [],
      },
    },
    components: {},
    data() {
      return {
        activeName: "",
      };
    },
    computed: {},
    watch: {
      $route: {
        handler: function (val) {
          this.activeName = val.path;
        },
        immediate: true,
      },
      tabs:{
        deep:true,
        handler(val){
          console.log(val,'新tabs');
          
        }
      }
    },
    created() {},
    mounted() {},
    methods: {
      tabClick(tab) {
        this.$router.push(tab.name);
      },
    },
  };
</script>
<style scoped lang="scss"></style>
