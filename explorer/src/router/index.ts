import { createRouter, createWebHistory } from 'vue-router';

const layoutCommonChildren = [
  {
    path: '',
    redirect: 'login',
    name: 'layout-child'
  },

  {
    path: 'dataIn',
    component: () => import('@/views/2_dataIn/index.vue'),
    redirect: '/dataIn/Task',
    children: [
      {
        path: ':tab',
        component: () => import('taos-ui/components/dataIn/views/dataTabs.vue')
      },
      {
        path: 'add',
        component: () => import('taos-ui/components/dataIn/views/sourceConfig.vue')
      },
      {
        path: ':taskId/:type/:page',
        component: () => import('taos-ui/components/dataIn/views/sourceConfig.vue')
      },
      {
        path: 'docs/:category/:lang',
        props: true,
        component: () => import('taos-ui/components/document/index.vue')
      }
    ]
  },
  {
    path: 'docs/:category/:lang',
    props: true,
    component: () => import('@/components/document/docs.vue')
  },
  {
    path: 'explorer',
    component: () => import('@/views/3_explorer/index.vue')
  },
  {
    path: 'profile',
    component: () => import('@/views/profile/index.vue')
  },
  {
    path: 'landing',
    component: () => import('@/views/landing/index.vue')
  },
  {
    path: 'programming',
    component: () => import('@/views/4_programming/index.vue'),
    children: [
      {
        path: '',
        component: () => import('@/views/4_programming/views/main.vue')
      },
      {
        path: 'docs/:category/:lang',
        props: true,
        component: () => import('@/components/document/docs.vue')
      }
    ]
  },
  {
    path: 'management',
    component: () => import('@/views/8_administrator/index.vue'),
    redirect: '/management/user',
    children: [
      {
        path: 'user',
        name: 'user',
        component: () => import('@/views/8_administrator/views/user.vue')
      },
      {
        path: 'backup',
        name: 'backup',
        component: () => import('@/views/8_administrator/views/backup.vue')
      },
      {
        path: 'replication',
        name: 'replication',
        component: () => import('@/views/8_administrator/views/replication.vue')
      },
      {
        path: 'cluster',
        name: 'cluster',
        component: () => import('@/views/8_administrator/views/cluster.vue')
      },
      {
        path: 'license',
        name: 'license',
        component: () => import('@/views/8_administrator/views/license.vue')
      },
      {
        path: 'audit',
        name: 'audit',
        component: () => import('@/views/8_administrator/views/audit.vue')
      },
      {
        path: 'slowSql',
        name: 'slowSql',
        component: () => import('@/views/8_administrator/views/slowSql.vue')
      }
    ]
  }
];
const adminRoute = [
  {
    path: 'dashboard',
    component: () => import('@/views/1_dashboard/index.vue')
  },
  {
    path: 'stream',
    meta: {
      role: ['1']
    },
    component: () => import('@/views/5_stream/index.vue')
  },
  {
    path: 'topic',
    meta: {
      role: ['1']
    },
    component: () => import('@/views/6_topic/index.vue'),
    children: [
      {
        path: '',
        props: true,
        component: () => import('@/views/6_topic/views/topic.vue')
      },
      {
        path: 'consumer',
        props: true,
        component: () => import('@/views/6_topic/views/consumer.vue')
      },
      {
        path: 'share',
        name: 'Topic Share',
        component: () => import('@/views/6_topic/views/shareTopic.vue')
      },
      {
        path: 'example',
        name: 'Topic Example',
        component: () => import('@/views/6_topic/views/example.vue')
      }
    ]
  },
  {
    path: 'tools',
    meta: {
      role: ['1']
    },
    component: () => import('@/views/7_tools/index.vue'),
    children: [
      {
        path: '',
        component: () => import('@/views/7_tools/views/main.vue')
      },
      {
        path: 'docs/:category/:lang',
        props: true,
        component: () => import('@/components/document/docs.vue')
      }
    ]
  }
];
const costantRoutes = [
  {
    path: '/',
    name: 'layout',
    component: () => import('@/layout/index.vue'),
    children: layoutCommonChildren
  },
  {
    path: '/:catchAll(.*)', // 不识别的path自动匹配404
    name: '404',
    component: () => import('@/views/404.vue')
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/0_login/index.vue')
  },
  {
    path: '/register',
    name: 'Register',
    component: () => import('@/views/0_login/register.vue')
  }
];

const router = createRouter({
  history: createWebHistory('/'),
  routes: costantRoutes
});

// router.onError(error => {
//   const jsPattern = /Loading chunk (\S)+ failed/g;
//   const cssPattern = /Loading CSS chunk (\S)+ failed/g;
//   const isChunkLoadFailed = error.message.match(jsPattern || cssPattern);
//   const targetPath = router.history.pending.fullPath;
//   if (isChunkLoadFailed) {
//     localStorage.setItem("targetPath", targetPath);
//     // window.location.reload();
//   }
// });
// router.onReady(() => {
//   const targetPath = localStorage.getItem("targetPath");
//   const tryReload = localStorage.getItem("tryReload");
//   if (targetPath) {
//     localStorage.removeItem("targetPath");
//     if (!tryReload) {
//       router.replace(targetPath);
//       localStorage.setItem("tryReload", true);
//     } else {
//       localStorage.removeItem("tryReload");
//     }
//   }
// });
// 添加主账户路由
export function addRoutes() {
  adminRoute.forEach(item => {
    router.addRoute('layout', item);
  });
}
addRoutes();
export default router;
