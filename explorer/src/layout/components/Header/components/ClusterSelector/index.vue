<template>
  <div class="cluster_selector" v-show="isShow">
    <div class="cluster_select_label">{{ $t("currentCluster") }}:</div>
    <el-select size="mini" v-model="current_cluster_id" placeholder="">
      <div v-for="item in clusters" :key="item.id">
        <el-option :label="item.alias || item.name" :value="item.id">
          <div class="flexBetween">
            <p>{{ item.alias || item.name }}</p>
            <Icon class="status-icon" :class="{ running: item.cluster_status == 'Running' }" name="status"></Icon>
          </div>
        </el-option>
      </div>
    </el-select>
  </div>
</template>

<script>
  import { mapState } from "vuex";
  import { NoInstanceSelectRoute } from "@/const";
  export default {
    data() {
      return {};
    },
    computed: {
      ...mapState({
        clusters: state => state.app.clusters,
      }),
      current_cluster_id: {
        get() {
          return this.$store.state.app?.current_cluster?.id;
        },
        set(val) {
          this.$store.commit(
            "app/SET_CURRENT_CLUSTER",
            this.clusters.find(item => {
              return String(item.id) == String(val);
            })
          );
        },
      },
      isShow() {
        return !NoInstanceSelectRoute.some(item => {
          return this.$route.path.startsWith(item);
        });
      },
    },
    methods: {},
  };
</script>

<style lang="scss" scoped>
  .cluster_selector {
    display: flex;
    flex-direction: row;
    align-items: center;
    :v-deep .el-input--mini {
      font-size: 18px;
    }
  }
  .flexBetween {
    height: 100%;
  }
  .status-icon {
    color: $divider-color;
    width: 25px;
    height: 25px;
    &.running {
      color: $color-success;
    }
  }
  .cluster_select_label {
    font-size: 18px;
    /* font-weight: 500; */
    /* min-width: 100px; */
    font-weight: normal;
    margin-right: 10px;
  }
</style>
