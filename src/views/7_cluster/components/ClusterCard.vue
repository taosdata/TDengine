<template>
  <div class="ClusterCard">
    <el-card class="box-card" shadow="always">
      <div v-if="role == '1'" class="cluster-wrapper">
        <div class="left">
          <div class="ClusterCard_Name_wrapper" @click.stop="handleEditcluster(cluster)">
            <p class="ClusterCard_NameText">
              {{ $t("name") }}:
              {{ cluster.alias || cluster.name }}
            </p>
            <i class="el-icon-edit ClusterCard_NameEditIcon"></i>
          </div>
          <div class="region" style="margin-top: 10px">
            <span class="label nowrap">Token: </span>
            <div class="region-content">
              {{ cluster.token.token }}
              <div class="cp-btn">
                <el-button type="text" size="mini" @click.stop="copy(cluster.token.token)" icon="el-icon-copy-document">{{ $t("copy") }}</el-button>
                <el-button type="text" size="mini" style="margin-left: 0" @click.stop="reset(cluster)" icon="el-icon-brush">{{
                  $t("reset")
                }}</el-button>
              </div>
            </div>
          </div>
        </div>
        <div class="center">
          <div class="region">
            <span class="label">{{ $t("dashboard.region") }}</span
            >: {{ cluster.cloud_name }} / {{ cluster.region_name }}
          </div>
          <div class="region">
            <span class="label nowrap">URL: </span>
            <div class="region-content">
              <span @click.stop target="_blank">{{ cluster.gateway_url }}</span>
              <el-button type="text" size="mini" class="cp-btn" @click.stop="copy(cluster.gateway_url)" icon="el-icon-copy-document">{{
                $t("copy")
              }}</el-button>
            </div>
          </div>
        </div>
        <div class="right" @click.stop>
          <div class="right-switch">
            <div class="ClusterCard_switch">
              <span class="active_text" v-if="handleStatuValue(cluster.cluster_status)">{{
                cluster.cluster_status == "Running" ? $t("dataIn.active") : cluster.cluster_status
              }}</span>
              <span class="inactive_text" v-else>{{ cluster.cluster_status == "Suspended" ? $t("dataIn.inactive") : cluster.cluster_status }}</span>
              <el-switch
                active-color="#4259CE"
                class="ClusterCard_switch_btn"
                :disabled="disable(cluster)"
                @change="handleChangeclusterState($event, cluster)"
                :value="handleStatuValue(cluster.cluster_status)"
              >
              </el-switch>
            </div>
            <div
              v-if="cluster.cluster_status == 'Running' && cluster.service_level != 'ENTERPRISE'"
              @click="upgrade"
              class="flexCenter"
              :title="$t('upgrade')"
            >
              <a href="javascript:void(0);">{{ $t("upgrade") }}</a>
            </div>

            <!-- <div>
              <i
                class="el-icon-delete ClusterCard_NameDelIcon"
                @click.stop="handleDeletecluster(cluster)"
              ></i>
            </div> -->
          </div>
          <div class="date">
            <span class="label">{{ $t("data.createAt") }}</span
            >: {{ cluster.create_time }}
          </div>
        </div>
      </div>
      <div v-else class="cluster-wrapper common-cluster">
        <div class="left">
          <div class="ClusterCard_Name_wrapper">
            <span class="ClusterCard_NameText">{{ $t("name") }}: {{ cluster.alias || cluster.name }}</span>
          </div>
          <div class="right-switch common">
            <div class="ClusterCard_switch">
              <span class="active_text" v-if="handleStatuValue(cluster.cluster_status)">{{
                cluster.cluster_status == "Running" ? $t("dataIn.active") : cluster.cluster_status
              }}</span>
              <span class="inactive_text" v-else>{{ cluster.cluster_status == "Suspended" ? $t("dataIn.inactive") : cluster.cluster_status }}</span>
              <el-switch
                active-color="#4259CE"
                class="ClusterCard_switch_btn"
                :disabled="true"
                @change="handleChangeclusterState($event, cluster)"
                :value="handleStatuValue(cluster.cluster_status)"
              >
              </el-switch>
            </div>
            <!-- <div>
              <i
                class="el-icon-delete ClusterCard_NameDelIcon"
                @click.stop="handleDeletecluster(cluster)"
              ></i>
            </div> -->
          </div>
        </div>
        <div class="center">
          <div class="region">
            <span class="label">{{ $t("dashboard.region") }}</span
            >: {{ cluster.cloud_name }} / {{ cluster.region_name }}
          </div>
          <div class="region">
            <span class="label nowrap">Token: </span>
            <div class="region-content">
              {{ cluster.token.token }}
              <div class="cp-btn">
                <el-button type="text" size="mini" @click.stop="copy(cluster.token.token)" icon="el-icon-copy-document">{{ $t("copy") }}</el-button>
                <el-button type="text" size="mini" style="margin-left: 0" @click.stop="reset(cluster)" icon="el-icon-brush">{{
                  $t("reset")
                }}</el-button>
              </div>
            </div>
          </div>
        </div>
        <div class="right" @click.stop>
          <div class="right-switch">
            <span class="label nowrap">URL: </span>
            <div class="region-content">
              <span @click.stop target="_blank" :href="cluster.gateway_url">{{ cluster.gateway_url }}</span>
              <el-button type="text" size="mini" class="cp-btn" @click.stop="copy(cluster.gateway_url)" icon="el-icon-copy-document">{{
                $t("copy")
              }}</el-button>
            </div>
          </div>
        </div>
      </div>
    </el-card>
  </div>
</template>

<script>
  import { copy, deepClone } from "@/utils";
  import { startCluster, stopCluster, resetToken } from "@/api/gateway/app";
  import { NoOperateStatus, InactiveStatus } from "@/const";
  export default {
    props: {
      cluster: {
        type: Object,
        default: () => {
          return {};
        },
      },
      tokenShow: {
        type: Boolean,
        default: false,
      },
    },
    data() {
      return {
        requestIng: false,
      };
    },
    computed: {
      role() {
        return this.$store.getters.role;
      },
    },
    methods: {
      handleEditcluster(cluster) {
        this.$emit("edit", deepClone(cluster));
      },
      handleDeletecluster(cluster) {
        this.$confirm(`${this.$t("del")} ${this.$t("dataIn.cluster")}: ${cluster.clusterName} ?`, "", {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(() => {
          this.$store.dispatch("api_cluster/deletecluster", String(cluster.id)).then(() => {
            this.$message.success(this.$t("delSucc"));
          });
        });
      },
      upgrade() {
        this.$emit("upgrade", this.cluster);
      },
      handleChangeclusterState(val, cluster) {
        this.$confirm(`${val ? this.$t("enable") : this.$t("disable")} ${this.$t("dashboard.cluster")}`, cluster.alias, {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          this.requestIng = true;
          const parmas = {
            id: cluster.id,
            price_level: cluster.service_level,
            cloudId: cluster.cloud_id,
            regionId: cluster.region_id,
          };
          let fn = val ? startCluster : stopCluster;
          await fn(parmas)
            .then(() => {
              this.$message.success(this.$t("operateSucc"));
            })
            .catch(() => false);
          this.requestIng = false;
          this.$store.dispatch("app/getClusterList");
        });
      },
      copy(text) {
        copy(text);
      },
      async reset(cluster) {
        if (this.requestIng) return;
        this.$confirm(`${this.$t("cluster.resetTokenTip").replace(/{ins_name}/, cluster.alias)} ?`, this.$t("wraning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          dangerouslyUseHTMLString: true,
          type: "warning",
        }).then(() => {
          this.requestIng = true;
          resetToken(cluster)
            .then(() => {
              this.$message.success(this.$t("resetSucc"));
            })
            .finally(() => {
              this.requestIng = false;
              this.$store.dispatch("app/getClusterList");
            });
        });
      },
      changeId() {
        this.$parent.tokenId = this.$parent.tokenId == this.cluster.id ? "" : this.cluster.id;
      },
      disable(cluster) {
        return NoOperateStatus.includes(cluster.cluster_status);
      },
      handleStatuValue(status) {
        return !InactiveStatus.includes(status);
      },
    },
  };
</script>

<style lang="scss" scoped>
  $left-width: 400px;
  .ClusterCard {
    margin-bottom: 1px;
    width: 100%;
    cursor: pointer;
    .cluster-wrapper {
      display: flex;
      justify-content: space-between;
      align-items: center;
      &.common-cluster {
        align-items: stretch;
      }
    }
    .left {
      flex: 0 0 30vw;
      overflow: hidden;
    }
    .center {
      flex: 2 2 200px;
      overflow: hidden;
    }
    .label {
      font-size: 16px;
      line-height: 26px;
      flex-shrink: 0;
    }
    .right {
      flex: 0.5 0 250px;
    }
    .right-switch {
      display: flex;
      align-items: center;
      line-height: 26px;
      &.common {
        margin-top: 10px;
      }
    }
    .ClusterCard_NameText {
      @extend .nowrap;
      color: #333;
      font-size: 18px;
      font-weight: 500;
      display: block;
    }
    .ClusterCard_NameEditIcon {
      min-width: 40px;
      flex: 1;
      margin-left: 15px;
      font-size: 16px;
      color: #606266;
    }
    .token-btn {
      font-size: 16px;
      line-height: 26px;
      margin-top: 10px;
      display: flex;
      align-items: center;
      &:hover {
        color: $color-primary;
      }
    }
    .ClusterCard_NameDelIcon {
      font-size: 20px;
      color: #606266;
      cursor: pointer;
      display: none;

      &:hover {
        color: $color-primary;
      }
    }
    & .ClusterCard_Name_wrapper {
      display: flex;
      flex-direction: row;
      align-items: center;
    }
    .ClusterCard_switch {
      color: #606266;
      // width: $left-width;
      padding-right: 10px;
      display: flex;
      flex-direction: row;
      font-size: 16px;
      align-items: center;
      .active_text {
        color: $color-primary;
      }
      .ClusterCard_switch_btn {
        margin-left: 8px;
        position: relative;
        top: 1px;
      }
    }
    & + .ClusterCard {
      margin-top: 30px;
    }
    &:hover {
      .ClusterCard_NameDelIcon {
        display: block;
      }
    }
  }
  .vpc-icon {
    width: 20px;
    height: 20px;
    vertical-align: middle;
    color: $color-primary;
  }
  .upgrade-icon {
    width: 20px;
    height: 20px;
    vertical-align: center;
    // margin-left: 10px;
    color: $color-primary;
    transform: rotate(-45deg);
  }
  .region {
    display: flex;
    align-items: center;
    // flex: 1;
    line-height: 26px;
    font-size: 16px;

    & + .region {
      margin-top: 10px;
    }
  }
  .region-content {
    flex: 1;
    @extend .nowrap;
    padding: 0 10px;
    position: relative;
    .cp-btn {
      display: none;
      position: absolute;
      right: 0;
      top: -5px;
      background-color: #fff;
    }
    &:hover {
      .cp-btn {
        display: block !important;
      }
    }
    & > a {
      line-height: 26px;
      color: $color-primary;
      text-decoration: underline;
    }
  }
  .date {
    margin-top: 10px;
    font-size: 16px;
  }
  .fade-enter-active,
  .fade-leave-active {
    transition: all 0.5s;
  }
  .fade-enter,
  .fade-leave-to {
    transform: scale(0);
    opacity: 0;
  }
</style>
