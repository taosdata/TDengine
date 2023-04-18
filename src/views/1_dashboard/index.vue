<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('dashboard.overview')"
      >
      <!-- <el-button
        slot="right"
        class="change-btn"
        @click="change"
        plain
        size="small"
      >
        {{ btnTitle }}
      </el-button> -->
      </MainContentHeader
    >
    <div class="content">
      <!-- <panelHeader :title="title"> </panelHeader> -->
      <docs :category="'dashboard'" :lang="'Dashboard'" ></docs>
      <!-- <ClusterInfo /> -->
      <!-- <docs :category="'tdinsight'" :lang="'tdinsight'" topic="TDinsight配置"></docs> -->
      <!-- <Query v-if="isQuery" />
      <template v-else>
        <ClusterInfo />
        <UsageTrend />
        </template> -->
    </div>
  </div>
</template>

<script>
import Docs from "@/components/Docs/index.vue";
import panelHeader from "@/components/panelHeader";
import { ClusterInfo, UsageTrend } from "./panels";
import Query from "@/views/sql/views/slow";
import moment from "moment";
import { SlowSqlTime } from "@/const";
export default {
  components: { ClusterInfo, UsageTrend, panelHeader, Query ,Docs},
  data() {
    return {
      isQuery: false,
    };
  },
  computed: {
    title() {
      return this.isQuery
        ? this.$t("sql.slowTitle").replace("{time}", SlowSqlTime)
        : this.$t("dashboard.totalUsage").replace(
            /Date/,
            moment.utc().format("YYYY-MM") + "-01"
          );
    },
    btnTitle() {
      return this.isQuery ? this.$t("dashboard.usage") : this.$t("route.sql");
    },
  },
  // mounted() {
  //   Guide({
  //     // 目标dom元素
  //     elements: [
  //       {
  //         target: "#menu_0", // required: true,   目标元素: 支持id ,class, ref 等
  //         text: "test", // 弹窗主文本内容
  //         placement: "right-top", // 弹窗方向位置 left-top | left-bottom | right-top | right-bottom | top-left | top-right | bottom-left | bottom-right
  //         image: "https://www.matools.com/img/home/gif/default_gif.gif", // 弹窗img | gif
  //         dialogWidth: 322, // 弹窗宽度  默认值 322
  //         // 标识
  //         sign: {
  //           show: true, // boolean 控制弹窗内是否显示标识   默认值 false
  //           text: "小贴士" // any 弹窗内的标识内容  默认值  小贴士
  //         },
  //         // 弹窗内 组件插槽
  //         slot: {
  //           //component: CustomComp, // 自定义组件
  //           // 自定义组件props
  //           props: {
  //             customProps: "我是自定义组件内容",
  //             handler() {
  //               alert("自定义组件方法触发成功");
  //             }
  //           },
  //           // 接收自定义组件内部emit事件
  //           listeners: {
  //             emitEvent() {
  //               alert("自定义组件emit事件触发成功");
  //             }
  //           }
  //         }
  //       },
  //       {
  //         target: "#menu_1", // required: true,   目标元素: 支持id ,class, ref 等
  //         text: "test", // 弹窗主文本内容
  //         placement: "right-top", // 弹窗方向位置 left-top | left-bottom | right-top | right-bottom | top-left | top-right | bottom-left | bottom-right
  //         image: "https://www.matools.com/img/home/gif/default_gif.gif", // 弹窗img | gif
  //         dialogWidth: 322, // 弹窗宽度  默认值 322
  //         // 标识
  //         sign: {
  //           show: true, // boolean 控制弹窗内是否显示标识   默认值 false
  //           text: "小贴士" // any 弹窗内的标识内容  默认值  小贴士
  //         },
  //         // 弹窗内 组件插槽
  //         slot: {
  //           //component: CustomComp, // 自定义组件
  //           // 自定义组件props
  //           props: {
  //             customProps: "我是自定义组件内容",
  //             handler() {
  //               alert("自定义组件方法触发成功");
  //             }
  //           },
  //           // 接收自定义组件内部emit事件
  //           listeners: {
  //             emitEvent() {
  //               alert("自定义组件emit事件触发成功");
  //             }
  //           }
  //         }
  //       }
  //     ],
  //     // 配置项
  //     config: {
  //       // 按钮类型支持
  //       btn: {
  //         pre: {
  //           type: "default", // 按钮类型 text, default, plaim, danger, primary   默认值 default
  //           text: "pre" //  默认值 上一条
  //         },
  //         next: {
  //           type: "primary", // 默认值 default
  //           text: "next" // 默认值 上一条
  //         },
  //         confirm: {
  //           type: "danger", // 默认值 default
  //           text: "confirm" // 默认值 知道啦
  //         }
  //       },
  //       // 遮罩颜色 默认值 rgba(0, 0, 0, 0.3)
  //       mask: {
  //         r: 0,
  //         g: 0,
  //         b: 0,
  //         a: 0.3
  //       },
  //       // 目标dom元素
  //       element: {
  //         // 边框颜色  默认值 rgba(33, 33, 33, 0.3)
  //         borderColor: {
  //           r: 33,
  //           g: 33,
  //           b: 33,
  //           a: 0.5
  //         },
  //         borderRadius: "4px" // 边框 角  默认值 '4px'
  //       },
  //       // 弹窗
  //       dialog: {
  //         // 背景色 默认值 rgba(255, 255, 255, 1)
  //         bgColor: {
  //           r: 255,
  //           g: 255,
  //           b: 255,
  //           a: 1
  //         },
  //         fontColor: "#000" // 字体颜色 默认值 #000
  //       }
  //     }
  //   });
  // },
  watch: {},
  methods: {
    change() {
      this.isQuery = !this.isQuery;
    },
  },
};
</script>

<style lang="scss" scoped>
.first_part_col {
  margin-bottom: 20px;
}
.change-btn {
  font-size: 16px;
}
</style>
