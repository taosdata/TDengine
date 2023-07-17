<template>
  <div class="dataOut-wrap flexStart"> 
    <el-form
      class="dataOut-form"
      :size="size"
      :rules="rules"
      label-width="200px"
      label-position="left"
      ref="form"
      :model="info"
    >
    <div class="td-source">
      <p class="flexStart radio-wrap">
        <el-radio-group :size="size" v-model="model">
          <el-radio-button label="Wizard"></el-radio-button>
          <el-radio-button label="SQL"></el-radio-button>
        </el-radio-group>
      </p>
      <template v-if="model == 'Wizard'">
        <el-form-item :label="$t('database')" prop="db">
          <el-select v-model="info.db">
            <el-option 
              v-for="item in dbList"
              :key="item.value"
              :label="item.label"
              :value="item.value">
            </el-option>
          </el-select>
        </el-form-item> 
        <el-form-item :label="$t('topic.stable')" prop="sTable">
          <el-select v-model="info.sTable">
            <el-option 
              v-for="item in dbList"
              :key="item.value"
              :label="item.label"
              :value="item.value">
            </el-option>
          </el-select>
        </el-form-item> 
        <el-form-item :label="$t('dataOut.startTime')" prop="startTime">
          <el-date-picker
            v-model="info.startTime"
            value-format="yyyy-MM-dd HH:mm:ss"
            type="datetime"
            :picker-options="startOption"
            placeholder="YYYY-MM-DD HH:mm:ss"
          >
          </el-date-picker>
        </el-form-item>  
        <el-form-item :label="$t('dataOut.endTime')" prop="endTime">
          <el-date-picker
            v-model="info.endTime"
            value-format="yyyy-MM-dd HH:mm:ss"
            type="datetime"
            :picker-options="endOption"
            placeholder="YYYY-MM-DD HH:mm:ss"
          >
          </el-date-picker>
        </el-form-item> 
        <!-- <el-form-item :label="$t('dataOut.endTime')" prop="endTime">
          <DatePicker
            v-model="info.endTime"
            value-format="yyyy-MM-dd HH:mm:ss"
            type="datetime"
            :picker-options="endOption"
            placeholder="YYYY-MM-DD HH:mm:ss"
          >
          </DatePicker>
        </el-form-item>  -->
        <el-form-item :label="$t('dataOut.cols')" prop="cols">
          <el-checkbox-group v-model="info.checkList">
            <el-checkbox label="ts"></el-checkbox>
            <el-checkbox label="col1"></el-checkbox>
            <el-checkbox label="col2"></el-checkbox>
          </el-checkbox-group>
        </el-form-item> 
        <el-form-item :label="$t('dataOut.tags')" prop="tags">
          <el-checkbox-group v-model="info.checkList">
            <el-checkbox label="tag1"></el-checkbox>
            <el-checkbox label="tag2"></el-checkbox>
            <el-checkbox label="tag3"></el-checkbox>
          </el-checkbox-group>
        </el-form-item> 
      </template>
      <!-- SQL start -->
      <!-- <template v-if="model == 'SQL'">
        <el-form-item :label="$t('topic.topicName')" required prop="topic_name">
          <el-input v-model="info.topic_name"> </el-input>
        </el-form-item>
      </template> -->
      <!-- SQL end -->
    </div>
    <div class="destination-source">
      <p class="flexStart radio-wrap">
        <el-radio-group :size="size" v-model="otherModel">
          <el-radio-button label="Kafka"></el-radio-button>
        </el-radio-group>
      </p>
      <template v-if="otherModel == 'Kafka'">
        <el-form-item :label="$t('dataOut.kafka')" prop="kafka">
          <el-input v-model="info.kafka"> </el-input>
        </el-form-item>
        <el-form-item :label="$t('dataOut.kafkaTopic')" prop="kafkaTopic">
          <el-input v-model="info.kafkaTopic"> </el-input>
        </el-form-item>
    </template>
    </div>
    <el-form-item>
      <div class="flexBetween">
        <el-button
          :loading="requestIng"
          :disabled="createBtn"
          type="primary"
          @click="submitForm"
          >{{ $t("submit") }}</el-button
        >
      </div>
    </el-form-item>
  </el-form>
  </div>
</template>

<script>
// import DatePicker from '@/components/date-picker'
// import { switchTimezone } from '@/utils/date-util'
import moment from 'moment-timezone'
  export default {
    // components: {DatePicker},
    data() {
        const startTimeOption = (time) => {
        if (this.info.endTime) {
          return time.getTime() > new Date(this.info.endTime).getTime();
        } else {
          return false;
        }
      };
      const endTimeOption = (time) => {
        if (this.info.startTime) {
          return time.getTime() < new Date(this.info.startTime).getTime();
        } else {
          return false;
        }
      };
      return {
        startOption: {
          disabledDate: (time) => startTimeOption(time),
        },

        endOption: {
          disabledDate: (time) => endTimeOption(time),
        },
        size: 'medium',
        info: {checkList: ['ts']},
        model: 'Wizard',
        otherModel: 'Kafka',
        dbList: ['DB1','db2'],
        requestIng: false,
        createBtn: false
      };
    },
    computed: {
      rules() {
        return {
          db: [{
            required: true, message: this.$t('pleaseSelect'),
          }],
          sTable: [{
            required: true, message: this.$t('pleaseSelect'),
          }],
          kafka: [{
            required: true, message: this.$t('dataIn.enterTip'),
          }],
          kafkaTopic: [{
            required: true, message: this.$t('dataIn.enterTip'),
          }],
        }
      }
    },
    mounted() {
      // console.log('moment.tz()',moment.tz(new Date(),'Asia/Shanghai').format());// 洛杉矶时间
      // console.log('moment()', typeof moment(new Date()).tz('America/Los_Angeles').format());// mo
      // console.log('moment(wwwww)', new Date(moment(new Date()).tz('America/Los_Angeles').format()));// mo
      // new Date(1689143227862)
      // let d = new Date()
      // let localTime = d.getTime()
      // let localOffset = d.getTimezoneOffset() * 60000
      // let utc = localTime + localOffset
      // let offset = 5.5
      // let bombay = utc + (3600000 * 5.5)
      // let d1 = new Date(bombay)
      // console.log('d',d,d1);
      // console.log('switchTimezone',switchTimezone(new Date()));
    },
    methods:{
      submitForm() {
        this.$refs.form.validate((valid) => {
          if (valid) {
            alert('submit!');
          } else {
            console.log('error submit!!');
            return false;
          }
        });
      },
    }
  };
</script>

<style lang="scss" scoped>
  .dataOut-wrap {

  }
  .dataOut-form {
    width: 800px;
    .radio-wrap {
      margin-bottom: 16px;
      border-bottom: 1px solid #eee;
    }
    ::v-deep .el-select {
      width: 100%;
    }
  }
  .td-source, .destination-source {
    border: 1px solid #ececef;
    margin-bottom: 20px;
    border-radius: 12px;
    padding: 15px;
  }
</style>
