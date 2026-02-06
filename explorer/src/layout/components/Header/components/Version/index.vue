<template>
  <div :class="['header-left', showHeaderLeft ? '' : 'hidden']" @click="clickShowVersion">
    <ul v-if="license[0]" class="license">
      <li>
        <!-- <span class="version">{{ $t(`header.${industry}`) }}：</span> -->
        <span class="value" :style="{ color: version.includes('Expired') ? 'red' : '' }">{{ version }}</span>
      </li>
    </ul>
  </div>
</template>
<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { getUser, getPassword, getClusterID, getBaseUrl } from '@/utils';
import { setInstanceData } from 'taos-ui/config';
import _ from 'lodash-es';

const props = withDefaults(defineProps<{
  statusBar?: boolean;
}>(), {
  statusBar: false
});

const route = useRoute();
const showHeaderLeft = ref<boolean>(true);
const clickCount = ref(0);
const clickNum = ref(0);
const license = ref<any>([]);
const local_version = localStorage.getItem('td_version') || '';
const version = ref(local_version || '0.0.0');
console.log('version', local_version, version.value);
const grants = ref<any>([]);

watch(
  () => route,
  (to, _from, next) => {
    try {
      if (to.name != 'Login') {
        getLicense();
      }
      next();
    } catch (error) {
      console.log('err');
    }
  },
  {
    immediate: true
  }
);

onMounted(() => {
  if (import.meta.env.VITE_APP_CUS_CONFIG) {
    const config = JSON.parse(import.meta.env.VITE_APP_CUS_CONFIG);
    if (Object.hasOwnProperty.call(config, 'serverVersionDisplay')) {
      showHeaderLeft.value = config?.serverVersionDisplay?.hide;
    }

    clickCount.value = config?.serverVersionDisplay?.showByClick;
  }
});

function clickShowVersion() {
  if (import.meta.env.VITE_APP_CUS_CONFIG) {
    clickNum.value++;
    if (clickNum.value > clickCount.value) return;
    if (clickNum.value == clickCount.value) {
      showHeaderLeft.value = true;
    }
  }
}
function getVersion(val: any) {
  if (val.match(/\./g).length > 3) {
    return val.substr(0, val.lastIndexOf('.'));
  } else {
    return val;
  }
}
async function getLicense() {
  try {
    const res = await sendSQLReq('show grants;');
    grants.value = res.data.map((data: any) => {
      return Object.fromEntries(
        res.column_meta.map((item:any, index: any) => {
          return [item[0], data[index]];
        })
      );
    });
    await sendSQLReq(`select server_version(), version from information_schema.ins_grants;`).then(res => {
      license.value = res.data.map((data: any) => {
        return Object.fromEntries(
          res.column_meta.map((item:any, index: any) => {
            return [item[0], data[index]];
          })
        );
      });
      const td_version = getVersion(license.value[0]['server_version()']);
      localStorage.setItem('td_version', td_version);

      setInstanceData({
        version: td_version,
        token: '',
        gatewayUrl: getBaseUrl(),
        id: '',
        ha: false,
        user: getUser(),
        password: getPassword(),
        tdClusterId: getClusterID()
      });
      let versionName = license.value[0]['version'];
      versionName = versionName.replace('official', '').trim();
      const fullVersion = versionName + ' ' + getVersion(license.value[0]['server_version()']);
      if (props.statusBar) {
        version.value = fullVersion;
      } else {
        version.value = versionName.replace('trial', '') + ' Explorer';
      }
      localStorage.setItem('serverVersion', fullVersion);
    });
  } catch (error: any) {
    console.error('Get license error: ', error);
    if (_.includes(error, 'Permission denied')) {
      license.value = [true];
      version.value = localStorage.getItem('td_version') || '';
      localStorage.setItem('serverVersion', version.value);
      console.log('No permission to view license information, using local version:', version.value);
      return;
    }
    ElMessage.error(error);
  }
}
</script>
<style scoped lang="scss">
.license {
  display: flex;

  span {
    font-size: 18px;
  }

  .value {
    color: #4259ce;
  }

  li {
    margin-right: 50px;
  }
}

.header-left.hidden {
  opacity: 0;
}
</style>
