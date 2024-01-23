<template>
  <div style="padding-bottom: 20px" v-loading="loading">
    <el-tabs v-model="activeName">
      <el-tab-pane v-for="item in datas" :value="item.name" :name="item.name" :key="item.name" :label="$t(`dataIn.${item.name}Metrics`)">
        <el-table size="mini" border :data="item.metrics">
          <el-table-column
            prop="name"
            show-overflow-tooltip
            :label="$t('dataIn.metricsName')"
            min-width="140"
          >
            <template slot-scope="{ row }">
              <span>{{ row.name }}</span>
            </template>
          </el-table-column>
          <el-table-column
            prop="metricsDesc"
            show-overflow-tooltip
            :label="$t('dataIn.metricsDesc')"
            min-width="300"
          >
            <template slot-scope="{ row }">
              <span>{{ metricsDesc[row.name] }}</span>
            </template>
          </el-table-column>
          <el-table-column
            prop="name"
            show-overflow-tooltip
            :label="$t('dataIn.metricsValue')"
            min-width="140"
          >
            <template slot-scope="{ row }">
              {{ handleValue(row) }}
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import { parseTime } from "@/utils";
export default {
  props: {
    data: {
      type: Array,
      default: () => [],
    },
    metricsDesc: {
      type: Object,
      default: () => {},
    },
    taskId: {
      type: Number,
    },
  },
  components: {},
  data() {
    return { 
      datas: [], 
      activeName: 'current',
      currentMetrics: [],
      totalMetrics: [],
      loading: true 
    };
  },
  computed: {},
  watch: {
    "$store.state.dialogVisible": {
      immediate: true,
      handler(val) {
        if (val) {
          this.connect();
        } else {
          this.disconnect();
        }
      }
    }
  },
  created() {},
  mounted() {console.log('metricsDesc',this.metricsDesc);},
  methods: {
    handleValue(data) {
      if (/start_time/i.test(data.name) && !isNaN(Number(data.value))) {
        return parseTime(data.value, "YYYY-MM-DD HH:mm:ss");
      } else if (['points_per_second','rows_per_second','total_points_per_second','total_rows_per_second'].includes(data.name)) {
        return Number(data.value).toFixed(2)
      } else {
        return data.value;
      }
    },
    connect() {
      this.disconnect();
      this.loading = true;
      this.activeName = 'current'
      const base_api = process.env.VUE_APP_BASE_URL
      let proto = ''
      let host = ''
      let wsUri = ''
      if (base_api) {
        proto = base_api.startsWith('https') ? 'wss' : 'ws';
        host = base_api.replace(/https?:\/\//, '')
      } else {
        const { location } = window;
        proto = location.protocol.startsWith("https") ? "wss" : "ws";
        host = location.host;
      }
      wsUri = `${proto}://${host}/api/x/metrics/task/${this.taskId}`

      this.socket = new WebSocket(wsUri);
      console.log("Connecting...");
      let array = [];
      if (this.socket) {
        this.socket.onerror = (err) => {
          console.log('Error', err);
          this.loading = false
          this.datas = []
        };
        this.socket.onmessage = (ev) => {
          let data = JSON.parse(ev.data);
          // console.log('Received: ' + data, data.current,ev, 'message')
          array = Object.keys(data).map((item) => ({
            name: item,
            value: data[item],
          }));
          this.datas = array.map(v => {
            let metrics = Object.keys(v.value).map((item) => ({
              name: item,
              value: v.value[item],
            }));
            return { name: v.name, metrics }
          })
          this.loading = false;
        };
      }
    },

    disconnect() {
      if (this.socket) {
        console.log("Disconnecting...");
        this.datas = []
        this.socket.close();
        this.socket = null;
        this.loading = false;
      }
    },
  },
};
</script>

<style scoped lang="scss"></style>
