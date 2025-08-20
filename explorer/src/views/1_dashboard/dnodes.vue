<template>
  <div>
    <header class="static-header">
      <span class="cluster-title">{{ $t('dashboard.cluster') }}</span>
      <span class="plain-text">{{ clusterID }}</span>
    </header>

    <div>
      <el-row>
        <el-col :span="6">
          <el-statistic title="taosd" :value="statisticData.taosd" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="taos-adapter" :value="statisticData.adapter" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="taosX" :value="statisticData.taosX" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="taos-keeper" :value="statisticData.keeper" />
        </el-col>
      </el-row>
    </div>
  </div>
  <el-divider />

  <div class="dnode">
    <div class="dnode-title">
      <span class="dnode-ep">{{ $t('dashboard.hosts') }}</span>
    </div>
    <el-table :data="dnodeList" style="width: 100%" stripe border height="100%">
      <el-table-column prop="ep" :label="$t('dashboard.endpoint')" width="240"></el-table-column>
      <el-table-column prop="res" :label="$t('dashboard.cpumem')" width="100"></el-table-column>
      <el-table-column prop="cpu_usage" :label="$t('dashboard.cpu_usage')" width="120">
        <template #default="scope">
          <span :class="`taos-status-${scope.row.cpu_usage_status}`">{{ scope.row.cpu_usage }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="mem_usage" :label="$t('dashboard.memory_usage')" width="120">
        <template #default="scope">
          <span :class="`taos-status-${scope.row.mem_usage_status}`">{{ scope.row.mem_usage }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="netio" :label="$t('dashboard.network')" width="180"></el-table-column>
      <el-table-column prop="diskio" :label="$t('dashboard.disk')" width="180"></el-table-column>
      <el-table-column prop="taosX" :label="$t('dashboard.service_status')">
        <template #default="scope">
          <span v-if="scope.row.taosd"
            >taosd: <i class="taos-icon-status" :class="`taos-status-server-${scope.row.taosd}`"></i
          ></span>
          <el-tooltip v-if="scope.row.adapter" :content="scope.row.adapter[0]" placement="top-start">
            <span
              >taos-adapter: <i class="taos-icon-status" :class="`taos-status-server-${scope.row.adapter[1]}`"></i
            ></span>
          </el-tooltip>
          <el-tooltip v-if="scope.row.taosX" :content="scope.row.taosX[0]" placement="top-start">
            <span>taosX: <i class="taos-icon-status" :class="`taos-status-server-${scope.row.taosX[1]}`"></i></span>
          </el-tooltip>
          <el-tooltip v-if="scope.row.keeper" :content="scope.row.keeper[0]" placement="top-start">
            <span
              >taos-keeper: <i class="taos-icon-status" :class="`taos-status-server-${scope.row.keeper[1]}`"></i
            ></span>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { getClusterID } from '@/utils';
import { t } from '@/lang/index';
const { $IS_COMMUNITY} = inject('globalCustomProperties') as GlobalCustomProperties;

const isCommunity = $IS_COMMUNITY;
const grafanaDashboard = ref(null);
const grafana_dashboards = localStorage.getItem('local_grafana');
if (grafana_dashboards) {
  grafanaDashboard.value = JSON.parse(grafana_dashboards);
}

const statisticData = ref({
  taosd: 0,
  adapter: 0,
  taosX: 0,
  keeper: 0
});

const clusterID = getClusterID();

const dnodeList = ref<any>([]);

function checkStatusByTime(ts: string, error_limit: string, warn_limit: string) {
  if (ts < error_limit) {
    return 'error';
  } else if (ts < warn_limit) {
    return 'warning';
  } else {
    return 'health';
  }
}

function checkStatusByMetrics(value: number) {
  if (value > 95) {
    return 'error';
  } else if (value > 80) {
    return 'warning';
  } else {
    return 'health';
  }
}

function formatKB(value: string): string {
  if (value === '0') {
    return '0';
  }
  const nvalue = Number(value) / 1000;
  return nvalue.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

async function loadDnodes() {
  const res = await sendSQLReq(
    'select (now-1d) as offline_limit, (now - 3m) as error_limit, (now - 1m) as warn_limit;'
  );
  if (!res && res.code !== 0) {
    return;
  }
  const [offline_limit, error_limit, warn_limit] = res.data[0];

  const cluster_res = await sendSQLReq(
    `select last_row(dnodes_total, dnodes_alive) from log.taosd_cluster_info where cluster_id = '${getClusterID()}';`
  );
  if (!cluster_res || cluster_res.code !== 0) {
    return;
  }
  if (Array.isArray(cluster_res.data) && cluster_res.data.length === 0) {
    ElMessage.warning("taosd cluster info empty");
    return
  }
  const [dnodes_total, _] = cluster_res.data[0];
  statisticData.value.taosd = dnodes_total;

  const dnode_res =
    await sendSQLReq(`select last_row(_ts, dnode_ep, cpu_cores, cpu_system, mem_total, mem_free, io_write_disk, io_read_disk, system_net_in, system_net_out ) 
 from log.taosd_dnodes_info where cluster_id = '${getClusterID()}' partition by dnode_ep`);

  const adpater_res = await sendSQLReq(
    `select last_row(ts, endpoint) from log.adapter_requests where ts > '${offline_limit}' and req_type=0 partition by endpoint;`
  );
  statisticData.value.adapter = adpater_res.data.length;
  const adapter_status: any = {};
  adpater_res.data.forEach((adapter: any) => {
    const [ts, endpoint] = adapter;
    const host = endpoint.split(':')[0];
    adapter_status[host] = [endpoint, checkStatusByTime(ts, error_limit, warn_limit)];
  });

  const taosx_status: any = {};
  let taosx_res;
  try {
    taosx_res = await sendSQLReq(
      `select last_row(_ts, taosx_id) from log.taosx_sys where _ts > '${offline_limit}' partition by taosx_id;`, false, !isCommunity 
    );
  } catch (e) {
    console.log(e);
  }

  if (taosx_res && taosx_res.code === 0) {
    statisticData.value.taosX = taosx_res.data.length;
    taosx_res.data.forEach((taosx: any) => {
      const [_ts, taosx_id] = taosx;
      const host = taosx_id.split(':')[0];
      taosx_status[host] = [taosx_id, checkStatusByTime(_ts, error_limit, warn_limit)];
    });
  }

  const keeper_res = await sendSQLReq(
    `select last_row(ts, identify) from log.keeper_monitor where ts > '${offline_limit}' partition by identify;`
  );
  statisticData.value.keeper = keeper_res.data.length;
  const keeper_status: any = {};
  keeper_res.data.forEach((keeper: any) => {
    const [ts, identify] = keeper;
    const host = identify.split(':')[0];
    keeper_status[host] = [identify, checkStatusByTime(ts, error_limit, warn_limit)];
  });

  dnodeList.value = dnode_res.data.map((dnode: any) => {
    const [
      _ts,
      ep,
      cpu_cores,
      cpu_system,
      mem_total,
      mem_free,
      io_write_disk,
      io_read_disk,
      system_net_in,
      system_net_out
    ] = dnode;
    const cpu_usage = cpu_system;
    const mem_usage = ((mem_total - mem_free) / mem_total) * 100;

    const netio = `${formatKB(system_net_in)} | ${formatKB(system_net_out)}`;
    const diskio = `${formatKB(io_read_disk)} | ${formatKB(io_write_disk)}`;
    const dnode_item: any = {
      ep,
      res: `${cpu_cores}cpu ${Math.round(mem_total / 1000000)}G`,
      cpu_usage_status: checkStatusByMetrics(cpu_usage),
      cpu_usage: `${cpu_usage.toFixed(2)}%`,
      mem_usage_status: checkStatusByMetrics(mem_usage),
      mem_usage: `${mem_usage.toFixed(2)}%`,
      netio,
      diskio
    };

    const host = ep.split(':')[0];
    if (taosx_status[host]) {
      dnode_item.taosX = taosx_status[host];
      delete taosx_status[host];
    }
    if (adapter_status[host]) {
      dnode_item.adapter = adapter_status[host];
      delete adapter_status[host];
    }
    if (keeper_status[host]) {
      dnode_item.keeper = keeper_status[host];
      delete keeper_status[host];
    }
    dnode_item.taosd = checkStatusByTime(_ts, error_limit, warn_limit);

    return dnode_item;
  });

  if (taosx_status) {
    Object.keys(taosx_status).forEach(host => {
      dnodeList.value.push({
        ep: taosx_status[host][0],
        taosX: taosx_status[host],
        res: '-',
        cpu_usage: '-',
        mem_usage: '-',
        netio: '-',
        diskio: '-'
      });
    });
  }

  Object.keys(adapter_status).forEach(host => {
    dnodeList.value.push({
      ep: adapter_status[host][0],
      adapter: adapter_status[host],
      res: '-',
      cpu_usage: '-',
      mem_usage: '-',
      netio: '-',
      diskio: '-'
    });
  });

  Object.keys(keeper_status).forEach(host => {
    dnodeList.value.push({
      ep: keeper_status[host][0],
      keeper: keeper_status[host],
      res: '-',
      cpu_usage: '-',
      mem_usage: '-',
      netio: '-',
      diskio: '-'
    });
  });
}

function tryLoadDNodes() {
  return loadDnodes().catch(error => {
    console.error(error);
    if (error.includes('Permission denied')) {
      ElMessage.error(t('dashboard.limited'));
      return;
    }
    ElMessage.error('Load dnodes error:', error.desc || error);
  });
}

tryLoadDNodes();
let timer: any = setInterval(() => {
  tryLoadDNodes();
}, 30000);

onUnmounted(() => {
  clearInterval(timer);
  timer = null;
});
</script>

<style lang="scss" scoped>
.static-header {
  .cluster-title {
    margin-right: 10px;
    font-size: 20px;
    font-weight: 900;
    color: $color-primary;
  }

  .plain-text {
    font-size: 14px;
    color: gray;
  }

  margin-bottom: 20px;
}

.dnode-title {
  .dnode-ep {
    margin-right: 10px;
    font-size: 20px;
    font-weight: 500;
    color: $color-primary;
  }

  margin-bottom: 20px;
}

.taos-icon-status {
  display: inline-block;
  width: 12px;
  height: 12px;
  margin-right: 20px;
  border-radius: 50%;
}

@keyframes blink {
  0%,
  100% {
    opacity: 1;
  }

  50% {
    opacity: 0.3;
  }
}

.taos-status-health {
  color: $color-success;
}

.taos-status-warning {
  color: $color-warning;
}

.taos-status-error {
  color: $color-danger;
}

.taos-status-server-health {
  background-color: $color-success;
}

.taos-status-server-warning {
  background-color: $color-warning;
  animation: blink 2.5s infinite;
}

.taos-status-server-error {
  background-color: $color-danger;
  animation: blink 2.5s infinite;
}

:deep(.el-statistic__head) {
  font-size: 14px;
}
</style>
