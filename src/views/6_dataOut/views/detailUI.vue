<template>
  <div
    :class="[
      'dataOut-wrap',
      'flexStart',
      (this.$parent.currentTaskStatus == 'running') && 'readable',
    ]"
  > 
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
          <!-- <el-radio-button label="SQL"></el-radio-button> -->
        </el-radio-group>
      </p>
      <template v-if="model == 'Wizard'">
        <div class="source-name" v-if="isEditable">
          <el-form-item :label="$t('datasource.targetname')">
            <el-input
              v-model="sourceName"
              placeholder=""
            ></el-input>
          </el-form-item>
        </div>
        <el-form-item :label="$t('database')" prop="sourceData.dbFiled">
          <el-select v-model="info.sourceData.dbFiled"  @change="dbChange" filterable>
            <el-option 
              v-for="item in dbList"
              :key="item.name"
              :label="item.name"
              :value="item.name">
            </el-option>
          </el-select>
        </el-form-item> 
        <el-form-item :label="$t('topic.stable')" prop="sourceData.table">
          <el-select 
            v-model="info.sourceData.table" 
            :remote-method="searchStable" 
            filterable
            remote
            :loading="requestIng"
            @focus="focus"
            @change="stbChange"
          >
            <el-option 
              v-for="item in stableList"
              :key="item.stable_name"
              :label="item.stable_name"
              :value="item.stable_name">
            </el-option>
          </el-select>
        </el-form-item> 
        <el-form-item 
          :label="$t('dataOut.cols')" 
          prop="sourceData.cols"
          >
          <el-checkbox-group v-model="info.sourceData.cols">
            <el-checkbox v-for="col in columns" :label="col.name" :key="col.name"></el-checkbox>
          </el-checkbox-group>
        </el-form-item> 
        <el-form-item :label="$t('dataOut.tags')" prop="sourceData.tags">
          <el-checkbox-group v-model="info.sourceData.tags">
            <el-checkbox v-for="col in tags" :label="col.name" :key="col.name"></el-checkbox>
          </el-checkbox-group>
        </el-form-item> 
        <el-form-item :label="$t('dataOut.startTime')" prop="sourceData.start">
          <DatePicker
            v-model="info.sourceData.start"
            value-format="yyyy-MM-dd HH:mm:ss"
            type="datetime"
            :picker-options="startOption"
            placeholder="YYYY-MM-DD HH:mm:ss"
          >
          </DatePicker>
        </el-form-item>  
        <el-form-item :label="$t('dataOut.endTime')" prop="sourceData.end">
          <DatePicker
            v-model="info.sourceData.end"
            value-format="yyyy-MM-dd HH:mm:ss"
            type="datetime"
            :picker-options="endOption"
            placeholder="YYYY-MM-DD HH:mm:ss"
          >
          </DatePicker>
        </el-form-item> 
        <el-form-item label="ts" prop="sourceData.ts"
          :rules="{
            required: requiredTs,
            message: $t('pleaseSelect'),
          }"
          >
          <el-input v-model="info.sourceData.ts" :placeholder="$t('dataOut.tsPlaceholder')"> </el-input>
        </el-form-item>
        <!-- <el-form-item label="Topic Suffix" prop="sourceData.topic_suffix"
          :rules="{
            required: true,
            message: $t('dataIn.enterTip'),
          }"
          >
          <el-input v-model="info.sourceData.topic_suffix"> </el-input>
        </el-form-item> -->
      </template>
      <!-- SQL start -->
      <!-- <template v-if="model == 'SQL'">
        <el-form-item :label="$t('topic.topicName')" required prop="topic_name">
          <el-input v-model="info.sourceData.topic_name"> </el-input>
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
        <el-form-item :label="$t('dataOut.kafka')" prop="target.kafkaUrl">
          <el-input v-model="info.target.kafkaUrl" placeholder="localhost:9092"> </el-input>
        </el-form-item>
        <el-form-item :label="$t('dataOut.kafkaTopic')" prop="target.topic">
          <el-input v-model="info.target.topic" :placeholder="$t('dataOut.topicPlaceholder')"> </el-input>
        </el-form-item> 
        <el-form-item :label="$t('dataOut.kafkaAckTimeout')" prop="target.ack_timeout">
          <el-input v-model="info.target.ack_timeout" :placeholder="$t('dataOut.timeoutPlaceholder')"> </el-input>
        </el-form-item>
        <el-form-item :label="$t('dataOut.kafkaBatchSize')" prop="target.batch_size">
          <el-input-number v-model="info.target.batch_size" :min="0"> </el-input-number>
        </el-form-item>
    </template>
    </div>
    <!-- <el-form-item> -->
      <div class="bottom">
        <el-button @click="cancel" class="cancel-btn">{{
          $t("cancel")
        }}</el-button>
        <el-button
          :loading="requestIng"
          :disabled="createBtn"
          type="primary"
          @click="submitForm"
          >{{ $t("submit") }}</el-button
        >
      </div>
    <!-- </el-form-item> -->
  </el-form>
  </div>
</template>

<script>
import DatePicker from '@/components/date-picker'
import { getDBListReq } from "@/api/gateway/data/dbs";
import { searchTable } from "@/api/gateway/data/tables";
import { searchStable, getStableStructReq } from "@/api/gateway/data/stables";
import { AddSource, EditSource } from "@/api/explorer/datain";
import { decrypt, deepClone } from "@/utils/index";
import { Message } from 'element-ui'

  export default {
    components: {DatePicker},
    data() {
        const startTimeOption = (time) => {
        if (this.info.sourceData.end) {
          return time.getTime() >= new Date(this.info.sourceData.end).getTime();
        } else {
          return false;
        }
      };
      const endTimeOption = (time) => {
        if (this.info.sourceData.start) {
          return time.getTime() <= (new Date(this.info.sourceData.start).getTime() - 24 * 60 * 60 * 1000);
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
        info: {
          sourceData: {
            dbFiled:'',
            table:'',
            cols: [],
            tags: [],
            start: '',
            end: '',
            ts: '',
            // topic_suffix: ''
          },
          target: {
            kafkaUrl: '',
            topic: '',
            ack_timeout: '',
            batch_size: ''
          },
        },
        model: 'Wizard',
        otherModel: 'Kafka',
        dbList: [],
        stableList: [],
        columns: [],
        tags: [],
        requestIng: false,
        createBtn: false,
        sourceName: localStorage.getItem('dataoutName'),
        checked: false,
        requiredTs: false
      };
    },
    props: {
      uidata: {
        type: Object,
        default: () => {},
      },
      isEditable: {
        type: Boolean,
        default: false,
      },
      editId: {
        type: Number,
        default: 0,
      },
    },
    computed: {
      rules() {
        return {
          'sourceData.dbFiled': [{
            required: true, message: this.$t('pleaseSelect'),
          }],
          'sourceData.table': [{
            required: true, message: this.$t('pleaseSelect'),
          }],
          'sourceData.cols': [{
            required: true, message: this.$t('pleaseSelect'),
          }],
          'target.kafkaUrl': [{
            required: true, message: this.$t('dataIn.enterTip'),
          }],
         'target.topic': [{
            required: true, message: this.$t('dataIn.enterTip'),
          }],
          'sourceData.start': [
            {
              validator: this.compareTime,
              trigger: "blur",  
            }
          ],
          'sourceData.end': [
            {
              validator: this.compareTime,
              trigger: "blur",  
            }
          ]
        }
      }
    },
    watch: {
      "info.sourceData.tags": {
        deep: true,
        handler(val) {
          if (val && Array.isArray(val) && val.length > 0) {
            this.checked = true
          } else {
            this.checked = false
          }
        }
      },
      "info.sourceData.start": {
        deep: true,
        handler(val) {
          if (val && this.info.sourceData.end) {
            this.requiredTs = true
          } else {
            this.requiredTs = false
          }
        }
      },
      "info.sourceData.end": {
        deep: true,
        handler(val) {
          if (val && this.info.sourceData.start) {
            this.requiredTs = true
          } else {
            this.requiredTs = false
          }
        }
      }
    },
    mounted() {
    },
    created() {
      this.getDBList();
      if (this.isEditable) {
        this.info = this.uidata
        this.getStableStruct()
      }
    },
    methods:{
      getDBList() {
        getDBListReq().then(data => {
          this.dbList = data;
          this.$emit("update:dbList", data);
        });
      },
      dbChange(val) {
        this.$emit("db-change", val);
      },
      searchStable(query) {
        if (this.requestIng) return;
        this.requestIng = true;
        searchStable(query, this.info.sourceData.dbFiled)
          .then(data => {
            this.stableList = data;
          })
          .catch(err => {
            this.stableList = [];
            err.desc && this.$error(err.desc);
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      stbChange(val) {
        this.getStableStruct()
      },
      async getStableStruct() {
        let data = await getStableStructReq({
          selected_db: this.info.sourceData.dbFiled,
          stableName: this.info.sourceData.table,
        }).catch(() => ({
          ts_field_name: "",
          columns: [],
          tags: [],
        }));
        this.columns = [{ name: data.ts_field_name, value: "timestamp" }].concat(
          data.columns.map((item) => ({ name: item.field, value: item.type }))
        );
        this.tags = data.tags.map((item) => ({
          name: item.field,
          value: item.type,
        }));

        if (!this.isEditable) {
          this.columns.map(item => this.info.sourceData.cols.push(item.name));
          this.tags.map(item => this.info.sourceData.tags.push(item.name));
        }
      },
      focus() {
        !this.info.sourceData.table && this.searchStable("");
      },
      //处理空值和‘undefined’字符值
      handleEmptyValue(val) {
        return (
          !Object.is(val, null) &&
          !Object.is(val, undefined) &&
          !Object.is(val, "") &&
          !Object.is(val, "undefined")
        );
      },
      cancel() {
        this.$parent.currentName = 'dbsource'
      },
      compareTime(_, value, callback) {
        let date1 = new Date(this.info.sourceData.start)
        let date2 = new Date(this.info.sourceData.end)
        if (date1 > date2) {
          return callback(new Error(this.$t('dataOut.startTime') + ' > ' + this.$t('dataOut.endTime')));
        } else {
          callback()
        }
      },
      submitForm() {
        this.$refs.form.validate(async (valid) => {
          if (valid) {
            let dns = "+http";
            let id = localStorage.getItem("local_clusterID");
            let username = localStorage.getItem("username")
            let pwd = decrypt(localStorage.getItem("pwd"));
            let baseUrl = localStorage.getItem("base_url")
            dns += `://`;
            if (this.handleEmptyValue(username)) {
              dns += `${username}`;
            }
            if (this.handleEmptyValue(pwd)) {
              dns += `:${pwd}`;
            }
            if (this.handleEmptyValue(baseUrl)) {
              dns += `@${baseUrl.replace(/https?:\/\//, "")}/${this.info.sourceData.dbFiled}`
            }
            let querystr = "";
         
            for (const key in this.info.sourceData) {
              if (Object.hasOwnProperty.call(this.info.sourceData, key) && this.info.sourceData[key] && key != 'dbFiled') {
                const element = this.info.sourceData[key];
                if (Array.isArray(element)) {
                  querystr += `${key}=${element.join()}&`
                } else {
                  querystr += `${key}=${element}&`
                }
              }
            }
            dns += querystr ? "?" + querystr.replace(/&$/g, "") : "";

            let toQuerystr = ''
            let { target } = this.info

            for (const key in target) {
              if (Object.hasOwnProperty.call(target, key) && target[key] 
                && !['kafkaUrl','topic'].includes(key)) {
                const element = target[key];
                if (Array.isArray(element)) {
                  toQuerystr += `${key}=${element.join()}&`
                } else {
                  toQuerystr += `${key}=${element}&`
                }
              }
            }
            toQuerystr = toQuerystr ? '?' + toQuerystr.replace(/&$/g, "") : ''

            let params = {
              from: 'tmq' + dns,
              to:
                `kafka://${target.kafkaUrl}/${target.topic}${toQuerystr}`,
              name: this.sourceName,
              labels: [
                "type::dataout",
                `cluster-id::${id}`,
                `user::${localStorage.getItem("username")}`,
              ],
            };
            if (this.isEditable) {
              let result = await EditSource(params, this.editId);
              if (result.message) {
                this.$error(result.message);
                return;
              }
              this.$parent.currentName = 'dbsource';
            } else {
              let result = await AddSource(params);
              if (result.message) {
                this.$error(result.message);
                return;
              }
              if (result && result.id) {
                Message.success("Operation Successfully!");
              }
              this.$parent.currentName = 'dbsource'
            }
            console.log('params',params);
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
  .dataOut-wrap.readable {
    position: relative;
    &::before {
      content: "";
      background: #f2f6fc40;
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 100;
    }
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
  :deep {
    .el-input-number__increase,
    .el-input-number__decrease {
      height: 38px;
      display: flex;
      justify-content: center;
      align-items: center;
    }
    .el-checkbox-group {
      display: flex;
      flex-wrap: wrap;
    }
    .el-checkbox {
      flex: 0 48%;
      margin-right: 1% !important;
    }
    .el-radio-button__inner {
      cursor: initial;
    }
  }
  .bottom {
    display: flex;
    border: none !important;
    padding: 0px !important;
    .el-button {
      flex: 1;
    }
    .el-select {
      margin-left: 0px !important;
    }
  }
    .cancel-btn {
      z-index: 101;
    }
</style>
