<template>
  <div class="sider" :class="sider_style">
    <SlideHeader />
    <el-menu
      ref="elMenu"
      :default-active="activeMenu"
      :collapse="isCollapse"
      active-text-color="rgb(37, 61, 172)"
      router
      class="sider-menu"
    >
      <template v-for="(routeItem, index) in permission_routes" :key="routeItem.path">
        <SiderMenuItem v-show="routeItem.meta.show" :id="'menu_' + index" :item="routeItem"></SiderMenuItem
      ></template>
    </el-menu>
  </div>
</template>

<script setup lang="ts">
import { SlideHeader, SiderMenuItem } from './components/index';
import useLicense from '@/hooks/useLicense';
import { getLocalLang } from '@/utils';
import { useStore } from 'vuex';
const { getMetaShow } = useLicense();
const store = useStore();
const route = useRoute();
const { $IS_OEM, $IS_COMMUNITY, $IS_TSDBLITE } = inject('globalCustomProperties') as GlobalCustomProperties;
const flag = $IS_OEM;

const isCollapse = ref<boolean>(false);
const permission_routes = ref([
  {
    path: '/dashboard',
    title: 'route.board',
    icon: 'dashboard',
    meta: {
      show: flag ? false : true
    }
    // role: ["1"],
  },
  {
    path: '/dataIn',
    title: 'route.dataIn',
    icon: 'dataIn',
    meta: {
      show: flag ? false : true //目前oem暂时不支持datain，后续根据taosx修改需要开放
    }
  },
  {
    path: '/explorer',
    title: 'route.explorer',
    icon: 'explorer',
    meta: {
      show: true
    }
  },
  {
    path: '/programming',
    title: 'route.programming',
    icon: 'programming',
    parting: false,
    meta: {
      show: flag ? false : true
    }
  },
  {
    path: '/stream',
    title: 'route.stream',
    icon: 'stream',
    role: ['1'],
    meta: {
      show: getMetaShow('stream')
    }
  },
  {
    path: '/topic',
    title: 'route.topic',
    icon: 'topic',
    role: ['1'],
    meta: {
      show: getMetaShow('subscription')
    }
  },

  {
    path: '/tools',
    title: 'route.tool',
    icon: 'tool',
    parting: true,
    meta: {
      show: flag ? false : true
    },
    role: ['1']
  },

  {
    path: '/management',
    title: 'route.admin',
    icon: 'admin',
    parting: false,
    meta: {
      show: localStorage.getItem('username') == 'root' ? true : false
    }
  }
]);

const sider_style = computed(() => {
  return store.state.sidebar.opened ? 'sider-unfold' : 'sider-fold';
});

const activeMenu = computed(() => {
  const { meta, path } = route;
  if (meta.activeMenu) {
    return meta.activeMenu;
  }
  return '/' + path.split('/')[1];
});

onMounted(() => {
  if (import.meta.env.VITE_APP_CUS_CONFIG && flag && JSON.parse(import.meta.env.VITE_APP_CUS_CONFIG).menus) {
    const menus = JSON.parse(import.meta.env.VITE_APP_CUS_CONFIG).menus;
    permission_routes.value = permission_routes.value.map(item => {
      if (Object.keys(menus).includes(item.path.replace('/', ''))) {
        item.title = getLocalLang().includes('zh')
          ? menus[item.path.replace('/', '')].zh
          : menus[item.path.replace('/', '')].en;
      }
      return item;
    });
  }

  if (!$IS_TSDBLITE && $IS_COMMUNITY) {
    store.commit('app/SET_SHOW_SYSTEM_MES', true);
  }
});
</script>

<style lang="scss" scoped>
.sider {
  position: relative;
  display: flex;
  flex-direction: column;
  height: 100%;
  background-color: #fff;
  box-shadow: rgb(0 0 0 / 5%) 0 -9px 9px;
  transition: width 0.4s ease 0s;
}

.sider-fold {
  width: 60px;
}

.sider-unfold {
  width: 240px;
}

.sider-menu {
  display: flex;
  flex: 1;
  flex-direction: column;
  margin-top: 14px;
  overflow: hidden auto;
  border-right: none;
}
</style>
