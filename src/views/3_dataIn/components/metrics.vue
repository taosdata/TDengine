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
      <el-tab-pane v-if="type == 'tmq'" :label="$t('dataIn.replicationProgress')" name='3'>
        <p class="title">{{ $t('dataIn.tbReplicationProgress') }}</p>
        <el-form :inline="true" :model="formInline" class="demo-form-inline" size="mini" ref="form" :rules="rules" >
          <el-form-item :label="$t('dataIn.tbName')" prop="table">
            <el-input style="width: 200px" :placeholder="$t('dataIn.tbNameP')" v-model="formInline.table"></el-input>
          </el-form-item>
          <el-form-item :label="$t('dataIn.timeRange')">
            <el-date-picker
              v-model="formInline.timeRange"
              value-format="timestamp"
              type="datetimerange"
              :start-placeholder="$t('start')"
              :end-placeholder="$t('end')">
            </el-date-picker>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="handleQurery" :loading="requesting_q">{{ $t('dataIn.query') }}</el-button>
          </el-form-item>
        </el-form>
        <el-table :data="tbReplicationData" size="mini" border>
          <el-table-column
            prop="table_name"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.table')"
            min-width="120"
          ></el-table-column>
          <el-table-column
            prop="from_last_ts"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.source')"
            min-width="200"
          >
            <template slot-scope="{ row }">
              <span>{{ row.from_last_ts ? parsinginZone(convertTsToMilliseconds(row.from_last_ts)) : 'null' }}</span>
            </template>
          </el-table-column>
          <el-table-column
            prop="to_last_ts"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.sink')"
            min-width="200"
          >
            <template slot-scope="{ row }">
              <span>{{ row.to_last_ts ? parsinginZone(convertTsToMilliseconds(row.to_last_ts)) : 'null' }}</span>
            </template>
          </el-table-column>
          <el-table-column
            prop="difference"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.difference')"
            min-width="130"
          >
          <template slot-scope="{ row }">
            <span>{{ formatDuration(row.from_last_ts, row.to_last_ts ) || 0 }}</span>
          </template>
          </el-table-column>
          <el-table-column
            prop="from_count"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.sourceNum')"
            min-width="180"
          ></el-table-column>
          <el-table-column
            prop="to_count"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.sinkNum')"
            min-width="170"
          ></el-table-column>
        </el-table>
        <br/>
        <br/>
        <p class="title">{{ $t('dataIn.vgroupReplicationProgress') }}</p>
        <div style="margin-bottom: 8px" class="flexBetween">
          <span>{{ $t('dataIn.updateTime') }} {{ parsinginZone(update_time) }}</span>
          <el-button @click="handleRefresh" :loading="requesting" size="mini" type="primary">{{ $t('dataIn.refresh')  }}</el-button>
        </div>
        <el-table :data="vgroupData" size="mini" border>
          <el-table-column
            prop="topic"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.topic')"
            min-width="140"
            :filters="filterMap.topic"
            :filter-method="filterHandler"
          ></el-table-column>
          <el-table-column
            prop="vgroup"
            sortable
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.vgroup')"
            min-width="140"
            :filters="filterMap.vgroup"
            :filter-method="filterHandler"
          >
          </el-table-column>
          <el-table-column
            prop="offset"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.offset')"
            min-width="140"
          ></el-table-column>
          <el-table-column
            prop="latest"
            show-overflow-tooltip
            :label="$t('dataIn.tbHeader.latest')"
            min-width="140"
          ></el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import { parseTime, parsinginZone } from "@/utils";
import moment from 'moment';
import { getTableProgress, getVgroupProgress } from '@/api/explorer/datain';
export default {
  props: {
    data: {
      type: Object,
      default: () => {},
    },
    metricsDesc: {
      type: Object,
      default: () => {},
    },
    taskId: {
      type: Number,
    },
    type: {
      type: String
    }
  },
  components: {},
  data() {
    return { 
      parseTime,
      parsinginZone,
      datas: [], 
      activeName: 'current',
      currentMetrics: [],
      totalMetrics: [],
      loading: true,
      requesting: false,
      requesting_q: false,
      update_time: '',
      formInline: {
        table: '',
        timeRange: ''
      },
      tbReplicationData: [],
      vgroupData: [],
    };
  },
  computed: {
    rules() {
      return {
        table: [
            { required: true, 
              message: this.$t("required", [this.$t('dataIn.tbName'),]), 
              trigger: 'blur'
            },
          ],
      } 
    },
    filterMap() {
      const topicFilteredArray = [];
      const vgroupFilteredArray = [];
      const seen = {};
      const seen1 = {};

      for (let item of this.vgroupData) {
        if (!seen[item.topic]) {
          topicFilteredArray.push({ text: item.topic, value: item.topic });
          seen[item.topic] = true;
        }
      }

      for (let item of this.vgroupData) {
        if (!seen1[item.vgroup]) {
          vgroupFilteredArray.push({ text: item.vgroup, value: item.vgroup });
          seen1[item.vgroup] = true;
        }
      }

      return {
        topic: topicFilteredArray,
        vgroup: vgroupFilteredArray
      }
    }
  },
  watch: {
    "$store.state.dialogVisible": {
      immediate: true,
      handler(val) {
        if (val) {
          this.handleMetricsData(this.data);
          this.connect();
          if (this.type == 'tmq') {
            this.tbReplicationData = [];
            this.vgroupData = [];
            this.handleRefresh()
          }
        } else {
          this.disconnect();
        }
      }
    }
  },
  methods: {
    handleValue(data) {
      if (/start_time/i.test(data.name) && !isNaN(Number(data.value))) {
        return parseTime(data.value, "YYYY-MM-DD HH:mm:ss");
      } else if (['points_per_second','rows_per_second','total_points_per_second','total_rows_per_second'].includes(data.name)) {
        return Number(data.value).toFixed(2)
      } else if (/execute_time/i.test(data.name)) {
        return this.formatDurationMs(data.value)
      } else {
        return data.value;
      }
    },
    convertTsToMilliseconds(timestamp) {
      // 判断时间戳位数
      if (timestamp && timestamp.toString().length >= 19) {
        return Number(String((timestamp / 1000000)).split('.')[0]); 
      } else if (timestamp && timestamp.toString().length > 13 && timestamp.toString().length <= 16) {
        return Number(String((timestamp / 1000)).split('.')[0]);
      } else {
        return timestamp; 
      }
    },
    
    formatDurationMs(durationInMs) {
      if (!durationInMs) return '';
      const duration = moment.duration(durationInMs);
      const years = Math.floor(duration.asYears());
      const months = duration.months();
      const days = duration.days();
      const hours = duration.hours();
      const minutes = duration.minutes();
      const seconds = duration.seconds();
      const milliseconds = duration.milliseconds()

      let formattedDuration = '';
      if (years > 0) {
        formattedDuration += years + this.$t('year');
      }
      if (months > 0) {
        formattedDuration += months + this.$t('month');
      }
      if (days > 0) {
        formattedDuration += days + this.$t('day');
      }
      if (hours > 0) {
        formattedDuration += hours + this.$t('hours');
      }
      if (minutes > 0) {
        formattedDuration += minutes + this.$t('minutes');
      }
      if (seconds > 0) {
        formattedDuration += seconds + this.$t('seconds');
      }
      if (milliseconds > 0) {
        formattedDuration += milliseconds + this.$t('milliseconds');
      }
      return formattedDuration;
    },

    formatDuration(from_last_ts, to_last_ts) {
      let from_time = this.convertTsToMilliseconds(from_last_ts)
      let to_time = this.convertTsToMilliseconds(to_last_ts)
      let diff_time = from_time - to_time
      let formattedDuration = this.formatDurationMs(diff_time)
      
      if (from_last_ts && from_last_ts.toString().length > 13 && from_last_ts.toString().length <= 16) {
        if (!to_last_ts) return ''
        let diffMicroseconds = Number(BigInt(String(from_last_ts)) - BigInt(String(to_last_ts))); // eslint-disable-line

        diffMicroseconds = diffMicroseconds % 1000;
        if (diffMicroseconds > 0) {
          formattedDuration += diffMicroseconds + this.$t('microseconds')
        } 
      }

      if (from_last_ts && from_last_ts.toString().length >=19) {
        if (!to_last_ts) return ''
        let diffNanoseconds = Number(BigInt(String(from_last_ts)) - BigInt(String(to_last_ts)));// eslint-disable-line
        let diffMicroseconds = Number(String((diffNanoseconds / 1000)).split('.')[0]) % 1000;

        console.log('diffNanoseconds',diffNanoseconds);
        
        if (diffMicroseconds > 0) {
          formattedDuration += diffMicroseconds + this.$t('microseconds')
        } 
        diffNanoseconds = diffNanoseconds % 1000;
        if (diffNanoseconds > 0) {
          formattedDuration += diffNanoseconds + this.$t('nanoseconds')
        } 
      }

      return formattedDuration;
    },

    connect() {
      this.disconnect();
      this.loading = false;
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
      
      let array = [];
      if (this.socket) {
        this.socket.onerror = (err) => {
          console.log('Error', err);
          // this.loading = false
          this.datas = []
        };
        this.socket.onmessage = (ev) => {
          let data = JSON.parse(ev.data);
          
          this.handleMetricsData(data);
        };
      }
    },

    handleMetricsData (metricsData) {
      let array = Object.keys(metricsData).map((item) => ({
        name: item,
        value: metricsData[item],
      }));
      this.datas = array.map(v => {
        let metrics = Object.keys(v.value).map((item) => ({
          name: item,
          value: v.value[item],
        }));
        return { name: v.name, metrics }
      })
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

    async handleRefresh() {
      try {
        this.requesting = true;
        let res = await getVgroupProgress(this.taskId)
        if (res && res.code && res.code !=0) {
          this.$error(res?.message);
          this.update_time = ""
          this.vgroupData = []
          return
        }
        this.update_time = res.update_time
        this.vgroupData = res.data
      } catch (error) {
        this.requesting = false;
      }
      this.requesting = false;
    },

    async handleQurery() {
      this.$refs.form.validate(async (valid) => {
        if (!valid) {
          this.requesting_q = false;
          return;
        }
        try {
          this.requesting_q = true;
          let { table, timeRange } = this.formInline
          let params = 'table' + '=' + table
          params += timeRange && timeRange.length > 0 
            ? `&start=${encodeURIComponent(parsinginZone(timeRange[0]))}&end=${encodeURIComponent(parsinginZone(timeRange[1]))}`
            : ''
          let res = await getTableProgress(this.taskId,params)
          if (res && res.code && res.code !=0) {
            this.$error(res?.message);
            this.tbReplicationData = [];
            return
          }
          this.tbReplicationData = [].concat(res)
        } catch (error) {
          this.requesting_q = false;
        }
      })
      this.requesting_q = false;
    },
    filterHandler(value, row, column) {
      const property = column['property'];
      return row[property] === value;
    }
  },
};
</script>

<style scoped lang="scss">
  .title {
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 14px;
  }
</style>
