<template>
  <div>
    <el-table size="mini" border :data="datas">
      <el-table-column
        prop="name"
        show-overflow-tooltip
        :label="$t('dataIn.metricName')"
        min-width="280"
      >
        <template slot-scope="{ row }">
          <el-tooltip placement="top" :content="metricsDesc[row.name]">
            <span>{{ row.name }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        prop="name"
        show-overflow-tooltip
        :label="$t('dataIn.metricValue')"
        min-width="180"
      >
        <template slot-scope="{ row }">
          {{ handleValue(row) }}
        </template>
      </el-table-column>
    </el-table>
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
    return { datas: [] };
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
  mounted() {},
  methods: {
    handleValue(data) {
      if (/timestamp/i.test(data.name) && !isNaN(Number(data.value))) {
        return parseTime(data.value, "YYYY-MM-DD HH:mm:ss");
      } else {
        return data.value;
      }
    },
    connect() {
      this.disconnect();
      // 测试用
      // const { location } = window;
      // const proto = location.protocol.startsWith("https") ? "wss" : "ws";
      // let host = "192.168.0.201:6050";
      // const wsUri = `${proto}://${host}/metrics/task/${this.taskId}`;

      const x_api = process.env.VUE_APP_X_API
      const proto = x_api.startsWith('https') ? 'wss' : 'ws'
      let host = proto == 'wss'
        ? x_api.replace('https',proto)
        : x_api.replace('http',proto);
      const wsUri = `${host}/metrics/task/${this.taskId}`

      this.socket = new WebSocket(wsUri);
      console.log("Connecting...");
      let array = [];
      if (this.socket) {
        this.socket.onerror = (err) => {
          console.log('Error', err);
        };
        this.socket.onmessage = (ev) => {
          let data = JSON.parse(ev.data);
          // console.log('Received: ' + data, ev, 'message')
          array = Object.keys(data).map((item) => ({
            name: item,
            value: data[item],
          }));
          this.datas = array;
        };
      }
    },

    disconnect() {
      if (this.socket) {
        console.log("Disconnecting...");
        this.socket.close();
        this.socket = null;
      }
    },
  },
};
</script>

<style scoped lang="scss"></style>
