<template>
  <div :class="['header-left', showHeaderLeft ? '' : 'hidden']" @click="clickShowVersion">
    <ul v-if="license[0]" class="license">
      <li>
        <span class="version">{{ $t(`header.${industry}`) }}：</span>
        <span class="value" :style="{ color: version.includes('Expired') ? 'red' : '' }">{{ version }}</span>
      </li>
    </ul>
  </div>
</template>
<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { getUser, getPassword, getClusterID, getBaseUrl } from '@/utils';
import { setInstanceData } from 'taos-ui/config';

const { OEM_NAME, $IS_TSDBLITE, $INDUSTRY } = inject('globalCustomProperties') as GlobalCustomProperties;
const route = useRoute();
const showHeaderLeft = ref<boolean>(true);
const clickCount = ref(0);
const clickNum = ref(0);
const license = ref<any>([]);
const local_version = localStorage.getItem('td_version') || '';
const version = ref(local_version || '0.0.0');
console.log('version', local_version, version.value);
const grants = ref<any>([]);
const industry = ref('version');

watch(
  () => route,
  (to, from, next) => {
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
function getVersion(val) {
  if (val.match(/\./g).length > 3) {
    return val.substr(0, val.lastIndexOf('.'));
  } else {
    return val;
  }
}
async function getLicense() {
  try {
    const res = await sendSQLReq('show grants;');
    grants.value = res.data.map(data => {
      return Object.fromEntries(
        res.column_meta.map((item, index) => {
          return [item[0], data[index]];
        })
      );
    });
    await sendSQLReq(
      `select server_version(), version, (expire_time < now) as valid from information_schema.ins_cluster;`
    ).then(res => {
      license.value = res.data.map(data => {
        return Object.fromEntries(
          res.column_meta.map((item, index) => {
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
      let versionName = '';
      switch (grants.value[0].version) {
        case 'trial':
        case `${OEM_NAME} Enterprise Edition trial`:
        case `TDengine Enterprise Edition trial`:
        case `${OEM_NAME} TSDB Enterprise Edition trial`:
        case `${OEM_NAME}-Enterprise trial`:
          versionName = license.value[0].valid ? 'Trial Expired' : 'Trial';
          break;
        case 'official':
        case `${OEM_NAME} Enterprise Edition official`:
        case `TDengine Enterprise Edition official`:
        case `${OEM_NAME} TSDB Enterprise Edition official`:
        case `${OEM_NAME}-Enterprise official`:
          versionName = license.value[0].valid ? 'Enterprise License Expired' : 'Enterprise';
          break;
        case `TDengine ${$INDUSTRY} Edition trial`:
        case `TDengine TSDB ${$INDUSTRY} Edition trial`:
          versionName = 'Trial';
          industry.value = 'power';
          break;
        case `TDengine ${$INDUSTRY} Edition official`:
        case `TDengine TSDB ${$INDUSTRY} Edition official`:
          versionName = 'Official';
          industry.value = 'power';
          break;
        default:
          versionName = $IS_TSDBLITE ? 'Lite' : 'OSS';
          break;
      }
      version.value = versionName + ' ' + getVersion(license.value[0]['server_version()']);
      localStorage.setItem('serverVersion', version.value);
    });
  } catch (error: any) {
    console.error('Get license error: ', error);
    if (error.includes('Permission denied')) {
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
