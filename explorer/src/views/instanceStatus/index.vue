<template>
  <div class="cluster-status">
    <section class="status-header">
      <SlideHeader />
      <div class="right">
        <LayoutHeader />
      </div>
    </section>
    <div class="status-content">
      <div class="content-wrapper">
        <h1 class="title">{{ $t("cluster.title") }}</h1>
        <div class="cluster-content">
          <el-steps align-center finish-status="success">
            <el-step
              v-for="(activity, index) in activities"
              :key="index"
              :title="activity.content"
              :icon="activity.icon"
              :style="{ color: activity.color }"
              :status="activity.state"
            >
            </el-step>
          </el-steps>
          <section class="form-content-wrapper info">
            <div class="left">
              <span class="label">{{ info.cloud }} :</span>
              <el-tooltip class="item" effect="light" :content="info.region" placement="top-start">
                <span class="value">{{ info.region }}</span>
              </el-tooltip>
            </div>
            <div class="right">
              <span class="label">{{ $t("clusterName") }} :</span>
              <el-tooltip class="item" effect="light" :content="info.clusterName" placement="top-start">
                <span class="value">{{ info.clusterName }}</span>
              </el-tooltip>
            </div>
          </section>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
  import { parseTime } from "@/utils/index";
  import LayoutHeader from "@/layout/components/Header";
  import { SlideHeader } from "@/layout/components/Sider/components";
  import { InitClusterStatus, InactiveStatus, BaseRoute } from "@/const";
  // import moment from "moment";
  export default {
    props: {
      appId: {
        type: String,
        default: "",
      },
    },
    components: {
      LayoutHeader,
      SlideHeader,
    },
    data() {
      return {};
    },
    computed: {
      currentCluster() {
        return this.$store.state.app.current_cluster;
      },
      activities() {
        const loading = "el-icon-loading";
        const ok = "el-icon-success";
        const wait = "el-icon-video-pause";
        let status = [
          {
            content: this.$t("cluster.start"),
            size: "large",
            status: "Starting",
            state: "wait",
          },
          {
            content: this.$t("cluster.running"),
            size: "large",
            status: "Running",
            state: "wait",
          },
        ];
        if (this.$route.query.isFirstCreate || this.currentCluster.cluster_status == "Ready") {
          status.unshift({
            content: this.$t("cluster.ready"),
            size: "large",
            status: "Ready",
            state: "process",
          });
        }
        let flag = false;
        status.forEach(item => {
          if (this.status == item.status) {
            item.icon = loading;
            item.color = "#409eff";
            item.state = "finish";
            flag = true;
            item.date = parseTime(Date.now(), "YYYY-MM-DD kk:mm:ss");
          } else {
            if (flag) {
              item.state = "wait";
              item.icon = wait;
              item.color = "#909399";
            } else {
              item.state = "success";
              item.icon = ok;
              item.color = "#67c23a";
              if (item.date) {
                item.date = parseTime(Date.now(), "YYYY-MM-DD kk:mm:ss");
              }
            }
          }
        });
        return status;
      },
      userInfo() {
        return this.$store.state.app.userInfo || {};
      },
      status() {
        return this.$store.state.app.clusterStatus;
      },
      info() {
        return {
          ...this.$store.getters.currentCloudAndRegion,
          clusterName: this.currentCluster.alias,
        };
      },
      currentId() {
        return this.appId || this.currentCluster.id;
      },
    },
    watch: {
      currentId: {
        immediate: true,
        handler() {
          this.decide();
        },
      },
    },
    methods: {
      decide() {
        if (InitClusterStatus.includes(this.status) || this.appId) {
          this.getStatus();
        } else {
          this.routerPush();
        }
      },
      async getStatus() {
        await this.$store.dispatch("app/getClusterStatus", this.appId);
        this.routerPush();
      },
      routerPush() {
        // 创建完第一个集群去教程页面
        if (this.$store.state.app.clusters.length == 1 && this.$route.query.isFirstCreate) {
          this.$alert(this.$t("cluster.createClusterSuccTip"), this.$t("cluster.congratulations"), {
            confirmButtonText: this.$t("continue"),
            dangerouslyUseHTMLString: true,
            showClose: false,
            customClass: "landing-tip-mexbox",
            callback: () => {
              this.$router.push("/landing");
            },
          });
        } else {
          let path = InactiveStatus.includes(this.status) ? "/instances" : "/";
          setTimeout(() => {
            if (window.location.pathname == "/instanceStatus" || !BaseRoute.some(item => window.location.pathname.includes(item))) {
              this.$router.push(path);
            }
          }, 1000);
        }
      },
      // 先根据集群创建时间进行判断是否为第一次创建
      // checkInstanceCreateTime() {
      //   const cluster = this.$store.state.app.current_cluster;
      //   const createTime = moment.utc(cluster.create_time).valueOf();
      //   return Date.now() < createTime + OffsetFirstCreateTime;
      // },
    },

    // beforeDestroy() {
    //   this.$store.commit("app/CLEAR_TIMEOUT");
    // },
  };
</script>

<style lang="scss" scoped>
  .cluster-status {
    display: flex;
    flex-direction: column;
    width: 100vw;
    height: 100vh;
    background-color: #fafbfd;
    &::v-deep .el-descriptions-item__label {
      width: 200px;
    }
    .descript-block {
      margin-top: 20px;
    }
  }
  .status-header {
    flex-shrink: 0;
    display: flex;
    .right {
      flex: 1;
    }
  }
  .title {
    font-size: 32px;
    line-height: 60px;
    margin-bottom: 40px;
    text-align: center;
    font-weight: normal;
  }

  .info {
    margin-top: 40px;
    &::v-deep .el-form-item__label {
      font-size: 26px;
      font-weight: normal;
    }
    &::v-deep .el-form-item__content {
      font-size: 18px;
    }
  }
  .status-content {
    // background-color: #fff;
    // min-width: 800px;
    @extend .flexCenter;
    flex: 1;
    padding: 30px 50px;
    // box-shadow: 0 2px 6px 0 rgb(80 87 107 / 16%);
  }
  .content-wrapper {
    width: 700px;
  }
  .form-content-wrapper {
    display: flex;
    justify-content: center;
    .left {
      margin-right: 20px;
      flex: 1 0 content;
    }
    .left,
    .right {
      display: flex;
      justify-content: center;
      font-size: 20px;
      line-height: 40px;
      @extend .nowrap;
      .value {
        margin-left: 20px;
        flex-shrink: 1;
      }
      .label {
        flex-shrink: 0;
      }
    }
    .right {
      flex: 1 1 content;
    }
  }
</style>
<style>
  .landing-tip-mexbox > .el-message-box__content {
    font-size: 16px;
  }
</style>
