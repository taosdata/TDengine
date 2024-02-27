import Vue from "vue";
import Router from "vue-router";


Vue.use(Router);
const layoutCommonChildren = [
  {
    path: "",
    redirect: "login",
  },

  {
    path: "dataIn",
    component: () => import("@/views/3_dataIn"),
    children: [
      {
        path: "",
        component: () => import("@/views/3_dataIn/views/main.vue"),
      },
      {
        path: "docs/:category/:lang",
        props: true,
        component: () => import("@/views/docs/index.vue"),
      },
      // {
      //   path: 'add',
      //   props: true,
      //   component: () => import("@/views/3_dataIn/views/dbSourceUI.vue")
      // },
    ],
  },
  {
    path: "docs/:category/:lang",
    props: true,
    component: () => import("@/views/docs/index.vue"),
  },
  {
    path: "explorer",
    component: () => import("@/views/2_explorer"),
  },
  // {
  //   path: "instances",
  //   component: () => import("@/views/7_cluster"),
  //   children: [
  //     {
  //       path: "",
  //       name: "ClusterList",
  //       component: () => import("@/views/7_cluster/views/list.vue"),
  //     },
  //     {
  //       path: "create",
  //       props: true,
  //       component: () => import("@/views/7_cluster/views/create.vue"),
  //     },
  //   ],
  // },

  {
    path: "support",
    component: () => import("@/views/support"),
    children: [
      {
        path: "",
        name: "supportList",
        component: () => import("@/views/support/views/list.vue"),
      },
      {
        path: "detail/:id",
        name: "supportDetail",
        props: true,
        component: () => import("@/views/support/views/detail.vue"),
      },
    ],
  },

  // {
  //   path: "dataOut",
  //   component: () => import("@/views/6_dataOut"),
  //   children: [
  //     {
  //       path: "",
  //       component: () => import("@/views/6_dataOut/views/main.vue"),
  //     },
  //     {
  //       path: "docs/:category/:lang",
  //       props: true,
  //       component: () => import("@/views/docs/index.vue"),
  //     },
  //   ],
  // },
  // {
  //   path: "visualize",
  //   component: () => import("@/views/visualize"),
  //   children: [
  //     {
  //       path: "",
  //       component: () => import("@/views/visualize/views/main.vue"),
  //     },
  //     {
  //       path: "docs/:category/:lang",
  //       props: true,
  //       component: () => import("@/views/docs/index.vue"),
  //     },
  //   ],
  // },
  {
    path: "profile",
    component: () => import("@/views/profile"),
  },
  {
    path: "landing",
    component: () => import("@/views/landing"),
  },
  // {
  //   path: "users",
  //   component: () => import("@/views/13_users"),
  // },
  {
    path: "backup",
    component: () => import("@/views/20_backup"),
  },
  {
    path: "programming",
    component: () => import("@/views/19_programming"),
    children: [
      {
        path: "",
        component: () => import("@/views/19_programming/views/main.vue"),
      },
      {
        path: "docs/:category/:lang",
        props: true,
        component: () => import("@/views/docs/index.vue"),
      },
    ]
  },
  // {
  //   path: "qnodes",
  //   component: () => import("@/views/16_qnodes"),
  // },
  // {
  //   path: "mnodes",
  //   component: () => import("@/views/15_mnodes"),
  // },
  // {
  //   path: "dnodes",
  //   component: () => import("@/views/14_dnodes"),
  // },


  // {
  //   path: "udf",
  //   component: () => import("@/views/17_udf"),
  // },
  {
    path: "management",
    component: () => import("@/views/13_administrator"),
  },
  // {
  //   path: "cluster",
  //   component: () => import("@/views/14_cluster"),
  // },
  // {
  //   path: "settings",
  //   component: () => import("@/views/18_settings"),
  // },
];
const adminRoute = [
  {
    path: "dashboard",
    component: () => import("@/views/1_dashboard"),
  },
  // {
  //   path: "replication",
  //   meta: {
  //     role: ["1"],
  //   },
  //   component: () => import("@/views/4_replication"),
  // },
  {
    path: "stream",
    meta: {
      role: ["1"],
    },
    component: () => import("@/views/10_stream"),
  },
  {
    path: "topic",
    meta: {
      role: ["1"],
    },
    component: () => import("@/views/11_topic"),
    children:[
      {
        path: "",
        props: true,
        component: () => import("@/views/11_topic/views/topic.vue"),
      },
      {
        path: "consumer",
        props: true,
        component: () => import("@/views/11_topic/views/consumer.vue"),
      },
      {
        path: "share",
        name: "Topic Share",
        component: () => import("@/views/11_topic/views/shareTopic.vue"),
      },
      {
        path: "example",
        name: "Topic Example",
        component: () => import("@/views/11_topic/views/example.vue"),
      },
    ]
  },
  {
    path: "tools",
    meta: {
      role: ["1"],
    },
    component: () => import("@/views/12_tools"),
    children: [
      {
        path: "",
        component: () => import("@/views/12_tools/views/main.vue"),
      },
      {
        path: "docs/:category/:lang",
        props: true,
        component: () => import("@/views/docs/index.vue"),
      },
    ],
  },
  {
    path: "/healthreport",
    component: () => import("@/views/21_healthreport"),
  },
  // {
  //   path: "user",
  //   component: () => import("@/views/9_user"),
  //   children: [
  //     {
  //       path: "",
  //       name: "userList",
  //       meta: {
  //         role: ["1"],
  //       },
  //       component: () => import("@/views/9_user/views/list"),
  //     },
  //     {
  //       path: "detail/:id",
  //       meta: {
  //         role: ["1"],
  //       },
  //       props: true,
  //       component: () => import("@/views/9_user/views/detail"),
  //     },
  //   ],
  // },
  // {
  //   path: "billing",
  //   component: () => import("@/views/8_billing"),
  //   children: [
  //     {
  //       path: "",
  //       component: () => import("@/views/8_billing/views/postpaid.vue"),
  //       meta: {
  //         role: ["1"],
  //       },
  //     },
  //   ],
  // },
  {
    path: "calculator",
    component: () => import("@/views/calculator"),
  },
  // {
  //   path: "network",
  //   component: () => import("@/views/5_VPC"),
  //   meta: {
  //     role: ["1"],
  //   },
  // },
  // {
  //   path: "activity",
  //   component: () => import("@/views/activity"),
  //   meta: {
  //     role: ["1"],
  //   },
  // },
  // {
  //   path: "alert",
  //   component: () => import("@/views/alert"),
  //   children: [
  //     {
  //       path: "",
  //       name: "alertList",
  //       component: () => import("@/views/alert/views/list"),
  //       meta: {
  //         role: ["1"],
  //       },
  //     },
  //   ],
  // },
];
const costantRoutes = [
  {
    path: "/instanceStatus/:appId?",
    name: "instanceStatus",
    props: true,
    component: () => import("@/views/instanceStatus/index.vue"),
  },
  
  {
    path: "/",
    name: "layout",
    component: () => import("@/layout"),
    children: layoutCommonChildren,
  },
  {
    path: "*",
    name: "404",
    component: () => import("@/views/404"),
  },
  {
    path: '/login',
    name:'Login',
    component: () => import("@/views/0_login")

  }
];

function createRouter(routes) {
  return new Router({
    mode: "history",
    routes,
  });
}
const router = createRouter(costantRoutes);
const RouterPush = Router.prototype.push;
Router.prototype.push = function (to) {
  return RouterPush.call(this, to).catch(err => err);
};
router.onError(error => {
  const jsPattern = /Loading chunk (\S)+ failed/g;
  const cssPattern = /Loading CSS chunk (\S)+ failed/g;
  const isChunkLoadFailed = error.message.match(jsPattern || cssPattern);
  const targetPath = router.history.pending.fullPath;
  if (isChunkLoadFailed) {
    localStorage.setItem("targetPath", targetPath);
    // window.location.reload();
  }
});
router.onReady(() => {
  const targetPath = localStorage.getItem("targetPath");
  const tryReload = localStorage.getItem("tryReload");
  if (targetPath) {
    localStorage.removeItem("targetPath");
    if (!tryReload) {
      router.replace(targetPath);
      localStorage.setItem("tryReload", true);
    } else {
      localStorage.removeItem("tryReload");
    }
  }
});
// 添加主账户路由
export function addRoutes(role) {
  // if (role != "1") return;
  adminRoute.forEach(item => {
    router.addRoute("layout", item);
  });
}
addRoutes()
export default router;
