<template>
  <div class="clusterInfo">
    <section class="usage-content" v-if="JSON.stringify(this.cluster_info)!=='{}'">
      <div class="info_card block-border">
        <div class="uptime-value flexCenter nowrap">{{ updateTime }}</div>
        <div class="info_label">{{ $t("dashboard.uptime") }}</div>
      </div>
      <div class="info_card block-border">
        <div
          :title="cluster_info.create_time"
          class="uptime-value flexCenter nowrap"
          style="font-size: 20px"
        >
          {{ cluster_info.create_time | handleValue }}
        </div>
        <div class="info_label">{{ $t("dashboard.createtime") }}</div>
      </div>
      <div class="info_card block-border">
        <div
          :title="cluster_info.expire_time"
          class="uptime-value flexCenter nowrap"
          style="font-size: 20px"
        >
          {{ cluster_info.expire_time | handleValue }}
        </div>
        <div class="info_label">{{ $t("dashboard.expiretime") }}</div>
      </div>
      <div class="info_card block-border">
        <!-- <div class="block-border"> -->
        <div
          :title="cluster_info.version"
          class="uptime-value flexCenter nowrap"
        >
          {{ cluster_info.version }}
        </div>
        <div class="info_label">
          {{ $t("dashboard.version") }}
          <!-- {{ $t("dashboard.storage") + " (GB " + $t("hour") + ")" }} -->
        </div>
        <!-- </div> -->
        <!-- <div class="block-border">
          <div :title="diskUsed" class="info_value">{{ diskUsed }}</div>
          <div class="info_label">{{ $t("dashboard.storage") + " (GB)" }}</div>
        </div> -->
      </div>
      <!-- <div class="info_card mini">
        <div class="block-border">
          <div :title="cluster_info.queryCount" class="info_value">
            {{ cluster_info.queryCount | handleValue }}
          </div>
          <div class="info_label">{{ $t("dashboard.queryNum") }}</div>
        </div>
        <div class="block-border">
          <div :title="cluster_info.insertCount" class="info_value">
            {{ cluster_info.insertCount | handleValue }}
          </div>
          <div class="info_label">{{ $t("dashboard.insertNum") }}</div>
        </div>
      </div> -->
    </section>
    <el-divider />
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
export default {
  components: {},
  data() {
    return {};
  },
  computed: {
    cluster_info() {
      return this.$store.state.app.cluster_info || {};
    },
    updateTime() {
      return this.handleUptime(this.cluster_info.uptime);
    },
    diskUsed() {
      return this.cluster_info?.diskUsed?.used?.toFixed(2) || 0;
    },
  },
  filters: {
    handleCreateTime(data) {
      if (!data) return "";
      return data.split(" ")[0] || "";
    },
    handleValue(val) {
      if (!val) return 0;
      let nval = Number(val);
      if (isNaN(nval)) return val;
      if (String(nval).includes(".")) {
        if (nval < 0.01) {
          return nval.toFixed(4);
        }
        nval = nval.toFixed(2);
        if (String(nval).endsWith(".00")) {
          nval = parseInt(nval);
        }
      }
      return nval;
    },
  },
  methods: {
    // 处理在线时间的展示
    handleUptime(time) {
      if (!time) return 0;
      // 目前返回的是秒
      if (time < 1) {
        time = 1;
      }
      time = parseInt(time);
      let unit = this.$t("dashboard.timeUnit");
      let dur = [60, 60, 24, 30];
      let index = 0;
      let fn = (time) => {
        if (time >= dur[index]) {
          // 当到达月的时候不再拼接
          if (index == dur.length) {
            return time + unit[index];
          }
          let re = time % dur[index] ? (time % dur[index]) + unit[index] : "";
          return fn(Math.floor(time / dur[index++])) + re;
        } else {
          return time + unit[index];
        }
      };
      return fn(time).match(/(\d+[^\d]+){1,2}/g)[0];
    },
    //没有配置grafana地址需要走接口展示页面
    async getDashData() {
      try {
        await sendSQLReq(`show cluster`).then((res) => {
          let result = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          this.$store.commit("app/SET_CLUSTER_INFO", result[0]);
        });
      } catch (error) {
        console.log(error);
      }
    },
  },
  created() {
    if (localStorage.getItem("local_grafana")==null) {
      this.getDashData();
    }else{
      this.$store.commit("app/SET_CLUSTER_INFO", null)
    }
  },
};
</script>

<style lang="scss" scoped>
$value-color: rgb(86, 166, 75);

.usage-content {
  margin-top: 10px;
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  overflow: auto;
  padding-bottom: 2px;
}
.info_card {
  width: 19.1%;
  min-width: 100px;
  // display: flex;
  // flex-direction: column;
  // align-items: center;
  // justify-content: center;
  text-align: center;
  &.mini {
    display: flex;
    flex-direction: column;
    .block-border {
      flex: 1;
      display: flex;
      flex-direction: column;
      .info_value {
        // flex: 1;
        @extend .flexCenter;
      }
    }
  }
}

.block-border {
  border: 1px solid $item-border-color;
  margin-top: 15px;
  display: flex;
  flex-direction: column;
  box-shadow: 0 1px 1px 0 var(--awsui-color-shadow-medium, rgba(0, 28, 36, 0.3)),
    0 1px 1px 0 var(--awsui-color-shadow-side, rgba(0, 28, 36, 0.15)),
    0 1px 1px 0 var(--awsui-color-shadow-side, rgba(0, 28, 36, 0.15));
}
.info_label {
  font-size: 16px;
  height: 36px;
  color: #000;
}

.info_value {
  font-size: 20px;
  line-height: 38px;
  color: $value-color;
}
.uptime-value {
  font-size: 40px;
  margin-top: 8px;
  line-height: 90px;
  color: $value-color;
  // flex: 1;
  // line-height: 200px;
  // font-weight: bold;
}
</style>
