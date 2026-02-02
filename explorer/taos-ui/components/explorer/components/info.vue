<template>
  <div class="info">
    <el-form label-width="180px" label-position="left">
      <section class="info-content">
        <el-form-item v-for="item in infoField" :key="item" :label="item + ':'">
          {{ infoData[item] }}
        </el-form-item>
        <el-form-item v-if="infoType !== 'db'" label="tags:"></el-form-item>
        <el-table
          v-if="infoType !== 'db'"
          style="width: 800px; margin-bottom: 40px"
          tooltip-effect="light"
          size="small"
          border
          :data="tags"
        >
          <el-table-column :show-overflow-tooltip="true" width="300" label="name" prop="name"> </el-table-column>
          <el-table-column :show-overflow-tooltip="true" :label="'type'" prop="type"> </el-table-column>
          <el-table-column v-if="infoType === 'tb'" :show-overflow-tooltip="true" label="value" prop="value">
          </el-table-column>
        </el-table>
        <el-form-item v-if="infoType == 'stb' || infoType == 'tb'" label="columns:"></el-form-item>
        <el-table
          v-if="infoType !== 'db' && columns?.length > 0"
          tooltip-effect="light"
          size="small"
          style="width: 800px; margin-bottom: 40px"
          border
          :data="columns"
        >
          <el-table-column :show-overflow-tooltip="true" width="300" label="name" prop="name"> </el-table-column>
          <el-table-column :show-overflow-tooltip="true" label="type" prop="type"> </el-table-column>
        </el-table>
      </section>
      <el-form-item v-if="infoType === 'db'" :label="'DSN:'" class="tmp-label">
        <div class="nowrap tmp-dns">{{ tmqDNS }}</div>
        <div class="cp-btn">
          <el-tooltip effect="light" :content="t('explorer.copyTMQDSN')">
            <el-button link type="primary" size="small" icon="copy-document" @click.stop="copyDNS()">{{
              t('common.copy')
            }}</el-button>
          </el-tooltip>
        </div>
      </el-form-item>
    </el-form>
  </div>
</template>

<script lang="ts" setup>
import { copy } from 'utils';
import { DBCustomedFiled } from 'taos-ui/constants/tdengine';
import { getCurrentInfoDataProvider } from './utils';
import { t } from 'locales';
import { getSubtbCurrentStruct, getStableStructReq } from '../../api';
import { instance, project } from 'config';

const displayMap = {
  stb: ['name', 'create_time'],
  tb: ['table_name', 'create_time', 'stable_name']
};
const columns = ref<Recordable[]>([]);
const tags = ref<Recordable[]>([]);
const currentInfoData = getCurrentInfoDataProvider();
const infoType = computed(() => currentInfoData.type);
const infoData = computed(() => currentInfoData[infoType.value]);
const infoField = computed(() =>
  infoType.value == 'db'
    ? Object.keys(infoData.value).filter((item: string) => !DBCustomedFiled.includes(item))
    : displayMap[infoType.value]
);
// const leftField = computed(() => infoField.value.filter((_, index: number) => index % 2 == 0));
// const rightField = computed(() => infoField.value.filter((_, index: number) => index % 2));
const tmqDNS = computed(() => {
  // return this.$store.state.app.current_cluster.urlPath;
  const gatewayURL = instance.gatewayUrl;
  const wsPrefix = gatewayURL.startsWith('https') ? 'wss' : 'ws';
  const uri = gatewayURL.replace(/https?:\/\//, '');
  const token = instance.token || '';
  const user = instance.user || '';
  const password = instance.password || '';
  const dbName = currentInfoData.db.name;
  if (project.isCloud && instance.token) {
    return `taos+${wsPrefix}://${uri}/${dbName}?token=${token}`;
  }
  return `taos+${wsPrefix}://${user}:${password}@${uri}/${dbName}`;
});
// watch(
//   () => infoData.value.name,
//   () => {
//     getStruct();
//   }
// );

function getStruct() {
  switch (infoType.value) {
    case 'stb':
      getStableStruct();
      break;
    case 'tb':
      getTableStruct();
      break;
    default:
      break;
  }
}

onMounted(() => {
  getStruct();
});

async function getStableStruct() {
  // 当为超级表的时候只需要获取结构的类型就可以了
  const data = await getStableStructReq(infoData.value.parent, infoData.value.name).catch(() => ({
    ts_field_name: '',
    columns: [],
    tags: []
  }));
  columns.value = processTagAndColumnData(data.columns);
  tags.value = processTagAndColumnData(data.tags);
}
async function getTableStruct() {
  const data = await getSubtbCurrentStruct(currentInfoData.db.name, '', infoData.value.name);
  tags.value = processTagAndColumnData(data.tags);
  columns.value = processTagAndColumnData(data.columns);
}

function processTagAndColumnData(data: Recordable[]) {
  return data.map((item: Recordable) => ({
    name: item.field,
    type: item.type,
    value: item.value
  }));
}
function copyDNS() {
  copy(tmqDNS.value);
}
</script>

<style lang="scss" scoped>
.info {
  height: 100%;
}

.tmp-label {
  &:deep(.el-form-item__label) {
    line-height: 40px !important;
  }

  &:deep(.el-form-item__content) {
    display: flex;
    align-items: center;
  }

  .tmp-dns {
    height: 40px;
    line-height: 40px;
  }
}
</style>
