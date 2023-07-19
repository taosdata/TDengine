<template>
  <div class="source-ui">
    <div
      :class="[
        'left-ui',
        this.$parent.currentTaskStatus == 'running' ? 'readable' : '',
      ]"
    >
      <section class="header">
        <h1>{{ dbsource[0].name ? dbsource[0].name : "" }}</h1>
      </section>
      <div class="source-name" v-if="isEditable">
        <div class="block-title">
          <span>{{$t('datasource.sourcename')}}</span>
        </div>
        <div class="name">
          <span class="label">{{$t('name')}}</span>
          <el-input
            v-model="sourceName"
            placeholder=""
            style="width: 200px"
          ></el-input>
        </div>
      </div>
      <section class="basics">
        <div class="block-title">
          <span>{{ dbsource[0].options.display }}</span>
        </div>
        <div class="protocol" v-if="dbsource[0].protocol">
          <span class="label">{{ dbsource[0].protocol.display }}</span>
          <div class="label-value">
            <el-select
              v-model="dbsource[0].protocol.value"
              placeholder=""
              style="margin-bottom: 8px"
            >
              <el-option
                v-for="c in dbsource[0].protocol.choices"
                :key="c.name"
                :label="c.display"
                :value="c.name"
              ></el-option>
            </el-select>
            <div
              v-html="transforHtml(dbsource[0].protocol.description)"
              class="description"
            ></div>
          </div>
        </div>
        <div class="first">
          <div style="width: 100%">
            <span
              :class="[
                'label',
                dbsource[0].options.host.required ? 'required' : '',
              ]"
              >{{ dbsource[0].options.host.display }}</span
            >
            <div class="label-value">
              <el-input
                v-model="dbsource[0].options.host.value"
                @change="changeHost(dbsource[0].options.host.display)"
                :placeholder="dbsource[0].options.host.placeholder"
                style="margin-bottom: 8px"
              ></el-input>
              <div
                v-html="transforHtml(dbsource[0].options.host.description)"
                class="description"
              ></div>
            </div>
          </div>
          <div
            style="width: 100%"
            v-if="dbsource[0].options.port && dbsource[0].options.port.display"
          >
            <span
              :class="[
                'label',
                dbsource[0].options.port.required ? 'required' : '',
              ]"
              >{{ dbsource[0].options.port.display }}</span
            >

            <div class="label-value">
              <el-input
                v-model="dbsource[0].options.port.value"
                :placeholder="dbsource[0].options.port.placeholder"
                @change="changePort"
                style="margin-bottom: 8px"
              ></el-input>
              <div
                v-html="transforHtml(dbsource[0].options.port.description)"
                class="description"
              ></div>
            </div>
          </div>
        </div>
        <div
          style="width: 100%"
          v-if="JSON.stringify(dbsource[0].options.subject) !== '{}'"
        >
          <span
            :class="[
              'label',
              dbsource[0].options.subject.required ? 'required' : '',
            ]"
            >{{ dbsource[0].options.subject.display }}</span
          >
          <div class="label-value">
            <el-input
              :placeholder="dbsource[0].options.subject.placeholder"
              v-model="dbsource[0].options.subject.value"
              style="margin-bottom: 8px"
            ></el-input>
            <div
              v-html="transforHtml(dbsource[0].options.subject.description)"
              class="description"
            ></div>
          </div>
        </div>
      </section>
      <section
        class="authentication"
        v-if="dbsource[0].authentication && dbsource[0].authentication.display"
      >
        <div>
          <div class="block-title">
            <span>{{ dbsource[0].authentication.display }}</span>
          </div>
          <div
            class="description"
            v-html="transforHtml(dbsource[0].authentication.description)"
          ></div>
        </div>
        <div class="authen-content">
          <el-tabs
            v-model="dbsource[0].authentication.value"
            @tab-click="handleClick"
          >
            <template v-for="at in dbsource[0].authentication.alternatives">
              <el-tab-pane :name="at.name" :key="at.name" :label="at.display">
                <template v-if="at.name == 'plain'">
                  <div class="plain">
                    <div class="plain-item">
                      <span class="label">{{
                        dbsource[0].authentication.alternatives[0].username
                          .display
                      }}</span>
                      <div style="flex: 1">
                        <el-input
                          style="margin-bottom: 8px"
                          v-model="
                            dbsource[0].authentication.alternatives[0].username
                              .value
                          "
                        ></el-input>
                        <p
                          class="description"
                          v-html="
                            transforHtml(
                              dbsource[0].authentication.alternatives[0]
                                .username.description
                            )
                          "
                        ></p>
                      </div>
                    </div>

                    <div class="plain-item">
                      <span class="label">{{
                        dbsource[0].authentication.alternatives[0].password
                          .display
                      }}</span>
                      <div style="flex: 1">
                        <el-input
                          type="password"
                          style="margin-bottom: 8px"
                          v-model="
                            dbsource[0].authentication.alternatives[0].password
                              .value
                          "
                        ></el-input>
                        <p
                          class="description"
                          v-html="
                            transforHtml(
                              dbsource[0].authentication.alternatives[0]
                                .password.description
                            )
                          "
                        ></p>
                      </div>
                    </div>
                  </div>
                </template>

                <div
                  v-else
                  v-for="(p, index) in at.params"
                  :key="index"
                  style="
                    width: 100%;
                    display: flex;
                    align-items: baseline;
                    margin-bottom: 8px;
                  "
                >
                  <span :class="['label', p.required ? 'required' : '']">{{
                    p.display
                  }}</span>

                  <div style="flex: 1">
                    <template v-if="p.hint && p.hint.choices">
                      <el-select
                        v-model="p.value"
                        placeholder=""
                        style="margin-left: 0px; width: 100%"
                      >
                        <el-option
                          v-for="c in p.hint.choices"
                          :key="c"
                          :label="c"
                          :value="c"
                        ></el-option>
                      </el-select>
                    </template>
                    <el-input
                      v-else
                      v-model="p.value"
                      :type="
                        p.name == 'password' || p.name == 'token'
                          ? 'password'
                          : 'text'
                      "
                      style="margin-bottom: 8px"
                    ></el-input>
                    <div
                      class="description"
                      v-html="transforHtml(p.description)"
                    ></div>
                  </div>
                </div>
              </el-tab-pane>
            </template>
          </el-tabs>

          <!-- <el-radio-group v-model="dbsource[0].authentication.value">
            <template v-for="at in dbsource[0].authentication.alternatives">
              <el-radio :key="at.name" :label="at.name"
                >{{ at.display }}
                <span class="des" style="color: #acaab2" v-if="at.description"
                  >({{ at.description }})</span
                >
              </el-radio>
            </template>
          </el-radio-group> -->
          <!-- <div class="authen-details">
            <template v-if="dbsource[0].authentication.value == 'plain'">
              <div class="plain">
                <div class="plain-item">
                  <span class="label">{{
                    dbsource[0].authentication.alternatives[0].username.display
                  }}</span>
                  <div style="width: 100%">
                    <el-input
                      v-model="
                        dbsource[0].authentication.alternatives[0].username
                          .value
                      "
                    ></el-input>
                    <p
                      class="description"
                      v-html="
                        transforHtml(
                          dbsource[0].authentication.alternatives[0].username
                            .description
                        )
                      "
                    ></p>
                  </div>
                </div>

                <div class="plain-item">
                  <span class="label">{{
                    dbsource[0].authentication.alternatives[0].password.display
                  }}</span>
                  <div style="width: 100%">
                    <el-input
                      type="password"
                      v-model="
                        dbsource[0].authentication.alternatives[0].password
                          .value
                      "
                    ></el-input>
                    <p
                      class="description"
                      v-html="
                        transforHtml(
                          dbsource[0].authentication.alternatives[0].password
                            .description
                        )
                      "
                    ></p>
                  </div>
                </div>
              </div>
            </template>
            <template v-else>
              <div
                v-for="al in dbsource[0].authentication.alternatives.filter(
                  (item) => item.name != 'plain'
                )"
                :key="al.name"
                style="
                  display: flex;
                  align-items: baseline;
                  flex-direction: column;
                "
              >
                <div
                  v-for="(p, index) in al.params"
                  :key="index"
                  style="width: 100%"
                >
                  <p>
                    <span class="label">{{ p.display }}</span>
                  </p>
                  <el-input v-model="p.value"></el-input>
                  <div
                    class="description"
                    v-html="transforHtml(p.description)"
                  ></div>
                </div>
              </div>
            </template>
          </div> -->
        </div>
      </section>
      <section
        :class="['groups-dataset', dbsource[0].datasets?.name]"
        v-if="dbsource[0]?.datasets"
      >
        <div style="flex-direction: column; align-items: baseline">
          <div class="block-title">
            <span>{{ dbsource[0].datasets.name }}</span>
          </div>
          <div
            class="description"
            v-html="transforHtml(dbsource[0].datasets.description)"
          ></div>
        </div>
        <template>
          <el-tabs v-model="activeName" @tab-click="handleClick">
            <el-tab-pane
              v-for="(p, pind) in dbsource[0].datasets.categories"
              :label="p.display"
              :name="p.category"
              :key="p.category"
              lazy
            >
              <div :key="pind">
                <div
                  class="description"
                  v-html="transforHtml(p.description)"
                ></div>
                <div class="target">
                  <span
                    :class="['no-label', p.target.required ? 'required' : '']"
                  ></span>
                  <template v-if="p.target.multiple">
                    <el-select
                      v-model="p.target.value"
                      :multiple="p.target.multiple"
                      :allow-create="p.target.editable"
                      :placeholder="p.target.placeholder"
                      filterable
                      default-first-option
                    >
                      <el-option
                        v-for="(t, tind) in p.target.value"
                        :key="tind"
                        :value="tind"
                        disabled
                      >
                        {{ t }}
                      </el-option>
                    </el-select>
                  </template>
                  <template v-else>
                    <el-input v-model="p.target.value"></el-input>
                  </template>
                  <el-button
                    size="medium"
                    @click="handleSelBtn"
                    style="height: 42px"
                    >{{ $t("datasource.select") }}</el-button
                  >
                </div>
                <div class="configuration" v-if="isShowConfiguration">
                  <el-input
                    :placeholder="$t('datasource.regexPlaceholder')"
                    v-model="p.value"
                    @keydown.enter.native="searchDatas"
                  ></el-input>
                  <div>
                    <div
                      class="searchList"
                      v-loading="loading"
                      v-if="configurationdata.length > 0"
                    >
                      <div
                        v-for="c in configurationdata"
                        :key="c.id"
                        :class="[activeDataSet.id == c.id ? 'actived' : '']"
                        @click="handelDataSet(c)"
                      >
                        {{ c.id }}
                      </div>
                    </div>
                    <template
                      v-if="
                        Object.hasOwnProperty.call(activeDataSet, 'options')
                      "
                    >
                      <div class="options-wrap">
                        <div class="option-list">
                          <div
                            class="option-item"
                            v-for="o in activeDataSet.options"
                            :key="o.name"
                          >
                            <span
                              :class="['label', o.required ? 'required' : '']"
                            >
                              {{ o.name }}
                            </span>
                            <el-input placeholder="" v-model="o.value" />
                          </div>
                        </div>
                        <div>
                          <el-button
                            size="small"
                            type="primary"
                            plain
                            @click="addOption"
                            >{{ $t("datasource.add") }}</el-button
                          >
                        </div>
                      </div>
                    </template>
                  </div>
                </div>
              </div>
            </el-tab-pane>
          </el-tabs>
        </template>
      </section>
      <template v-for="item in dbsource[0].groups">
        <section :class="['groups', item.name]" :key="item.display_order">
          <div style="flex-direction: column; align-items: baseline">
            <div class="block-title">
              <span>{{ item.display ? item.display : item.name }}</span>
            </div>
            <div
              class="description"
              v-html="transforHtml(item.description)"
            ></div>
          </div>
          <template v-for="(p, pind) in item.params">
            <div :key="pind">
              <span :class="['label', p.required ? 'required' : '']">
                {{ p.display ? p.display : p.name }}
              </span>
              <div class="label-value">
                <template
                  v-if="
                    p.hint === 'str' ||
                    p.hint === 'timeout' ||
                    p.hint.type == 'timeout'
                  "
                >
                  <el-input v-model="p.value" placeholder=""></el-input>
                </template>
                <template v-if="p.hint.type && p.hint.type === 'str'">
                  <div v-if="p.hint.choices" class="select-with-btn">
                    <el-select
                      v-if="['bucket','measurements'].includes(p.name)"
                      v-model="p.value"
                      placeholder=""
                      :style="
                        p.name === 'bucket' 
                        ? {width: '80%', marginLeft: '-15px',marginRight: '8px'} 
                        : {marginLeft: '-15px'}
                      "
                      :allow-create="true"
                      filterable
                      default-first-option
                      :multiple="p.multiple"
                      @change="value => changeBucket(value,p.name)"
                    >
                      <el-option
                        v-for="c in (p.name == 'bucket') ? bucketList: measurementList[0]?.children"
                        :key="c.id || c"
                        :label="c.id || c"
                        :value="c.id || c"
                      ></el-option>
                    </el-select>
                    <el-select
                      v-else
                      v-model="p.value"
                      placeholder=""
                      style="margin-left: -15px;"
                    >
                      <el-option
                        v-for="c in p.hint.choices"
                        :key="c"
                        :label="c"
                        :value="c"
                      ></el-option>
                    </el-select>
                    <el-button 
                      v-if="p.name === 'bucket'" 
                      size="medium" type="primary" plain 
                      :disable="btnLoading"
                      :loading="btnLoading"
                      @click="getSchema">获取 Schema</el-button>
                  </div>
                  <el-input v-else v-model="p.value"></el-input>
                </template>
                <template v-if="p.hint === 'bool' || p.hint.type === 'bool'">
                  <!-- <el-radio-group v-model="p.value">
                    <el-radio v-for="c in p.choices" :key="c" :label="c">
                      {{ c }}
                    </el-radio>
                  </el-radio-group> -->
                  <el-checkbox
                    v-model="p.value"
                    true-label="true"
                    false-label="false"
                  ></el-checkbox>
                </template>
                <template
                  v-if="
                    (p.hint.type && p.hint.type === 'integer') ||
                    p.hint === 'integer'
                  "
                >
                  <el-input-number
                    v-model="p.value"
                    :min="p.hint.min"
                    :max="p.hint.max"
                  ></el-input-number>
                </template>
                <template v-if="p.hint == 'time' || p.hint?.type == 'time'">
                  <DatePicker
                    v-model="p.value"
                    value-format="yyyy-MM-dd HH:mm:ss"
                    type="datetime"
                    v-if="p.name == 'beginTime' || p.name == 'endTime'"
                    :picker-options="
                      p.name == 'beginTime' ? startOption : endOption
                    "
                    :placeholder="p.placeholder"
                  >
                  </DatePicker>
                  <DatePicker
                    v-model="p.value"
                    value-format="yyyy-MM-dd HH:mm:ss"
                    type="datetime"
                    v-if="
                      p.name == 'BackfillStartTime' ||
                      p.name == 'BackfillEndTime'
                    "
                    :picker-options="
                      p.name == 'BackfillStartTime'
                        ? backfillStartOption
                        : backfillEndOption
                    "
                    :placeholder="p.placeholder"
                  >
                  </DatePicker>
                </template>
                <!-- <template v-if="p.hint?.type == 'datetime'">
                  <el-date-picker
                    v-model="p.value"
                    type="datetime"
                    placeholder="Please select the date"
                  >
                  </el-date-picker>
                </template> -->
                <div
                  v-html="transforHtml(p.description)"
                  class="description"
                ></div>
              </div>
            </div>
          </template>
        </section>
      </template>

      <!--未分组显示根节点下的params，显示方式和groups一样-->
      <section class="ungrounded" v-if="dbsource[0].params"></section>
      <section class="choose-db">
        <span class="label required">{{ this.$t("datasource.targetdb") }}</span>
        <div class="target-db-name">
          <el-select v-model="dbname" placeholder="">
            <el-option
              v-for="db in dblist"
              :key="db['node-key']"
              :label="db.name"
              :value="db.name"
            ></el-option>
          </el-select>
          <!-- <span class="desc">{{$t('datasource.influxdbtip')}}</span> -->
        </div>
      </section>
      <section class="bottom">
        <el-button type="primary" @click="submit" :disabled="disable">{{
          $t("submit")
        }}</el-button>
      </section>
    </div>
    <div class="right-ui">
      <mavon-editor
        v-model="dbsource[0].description"
        :toolbarsFlag="false"
        :default-open="'preview'"
        :subfield="false"
      />
    </div>
  </div>
</template>
<script>
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { AddSource, EditSource, getUaAndDaData } from "@/api/explorer/datain";
import DatePicker from '@/components/date-picker'
import { Message } from "element-ui";
import marked from "marked";
import { debounce, parsinginZone } from "@/utils/index";
export default {
  name: "DbSourceUI",
  components: {DatePicker},
  props: {
    // sourceName: {
    //   type: String,
    //   default: "",
    // },
    tagName: {
      type: String,
      default: "datasource",
    },
    dbsourceList: {
      type: Array,
      default() {
        return [];
      },
    },
    isEditable: {
      type: Boolean,
      default: false,
    },
    editId: {
      type: Number,
      default: 0,
    },
    dbName: {
      type: String,
      default: "",
    },
  },

  data() {
    const startTimeOption = (time) => {
      let end = this.dbsource[0].groups[0].params.filter(
        (item) => item.name == "endTime"
      );
      if (end[0].value) {
        return time.getTime() > new Date(end[0].value).getTime();
      } else {
        return false;
      }
    };
    const endTimeOption = (time) => {
      let start = this.dbsource[0].groups[0].params.filter(
        (item) => item.name == "beginTime"
      );
      if (start[0].value) {
        return time.getTime() < new Date(start[0].value).getTime();
      } else {
        return false;
      }
    };
    const backfillStart = (time) => {
      let end = this.dbsource[0].groups
        .filter((val) => val.name == "Backfill")[0]
        .params.filter((item) => item.name == "BackfillEndTime");
      if (end[0].value) {
        return time.getTime() > new Date(end[0].value).getTime();
      } else {
        return false;
      }
    };
    const backfillEnd = (time) => {
      let start = this.dbsource[0].groups
        .filter((val) => val.name == "Backfill")[0]
        .params.filter((item) => item.name == "BackfillStartTime");
      if (start[0].value) {
        return time.getTime() < new Date(start[0].value).getTime();
      } else {
        return false;
      }
    };
    return {
      sourceName:localStorage.getItem('datainName'),
      startOption: {
        disabledDate: (time) => startTimeOption(time),
      },

      endOption: {
        disabledDate: (time) => endTimeOption(time),
      },
      backfillStartOption: {
        disabledDate: (time) => backfillStart(time),
      },
      backfillEndOption: {
        disabledDate: (time) => backfillEnd(time),
      },
      //判断ip和域名
      ipRegex:
        /^(?=^.{3,255}$)(http(s)?:\/\/)?(www\.)?[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+(:\d+)*(\/\w+\.\w+)*$/,
      // /^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$/,
      isIP: true,
      isPort: true,
      disable: false,
      address: "",
      port: "",
      username: "",
      password: "",
      subject: "",
      radio: "",
      dblist: [],
      dbname: "",
      activeName: "",
      textarea: "",
      isShowConfiguration: false,
      loading: false,
      configurationdata: [],
      activeDataSet: {},
      dbsource: [],
      btnLoading: false,
      bucketList: [],
      measurementList: []
    };
  },
  created() {
    this.getDatabases();
    this.dbsource = this.dbsourceList;
    if (this.isEditable) {
      this.dbname = this.dbName;
      this.handleEditData()
    }
  },
  mounted() {
    this.activeName = this.dbsource[0].datasets
      ? this.dbsource[0].datasets.categories[0].category
      : "";
  },
  watch: {
    dbName: {
      deep: true,
      handler(val) {
        if (this.isEditable) {
          this.dbname = this.dbName;
        }
      },
    },
  },
  methods: {
    handleEditData() {
      this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
        group.params.map((p) => {
          if ((p.hint === 'time' || p.hint?.type === 'time') && p.value) {
            // 时间返回值适配时区后再根据 placeholder字段 格式化
            p.value = parsinginZone(p.value, p.placeholder)
          }
          if (p.multiple && p.value && typeof p.value =='string') {
            // 多选下拉框的返回值改为数组
            let newVal = p.value.split()
            p.value = newVal
          }
            return p
          });
          return group
        });
    },
    changeHost(host) {
      if (this.tagName == "influxdb") {
        this.isIP = this.ipRegex.test(this.dbsource[0].options.host.value);
      }
    },
    changePort() {
      this.isPort =
        /^([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-4]\d{4}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])$/.test(
          this.dbsource[0].options.port.value
        );
    },
    transforHtml(val) {
      if (val) {
        return marked.parse(val);
      } else {
        return val;
      }
    },
    getRequiredItem(source) {
      if (!source && typeof source !== "object") {
        throw new Error("error arguments", "deepClone");
      }
      const targetObj = source.constructor === Array ? [] : {};
      Object.keys(source).forEach((keys) => {
        if (source[keys] && typeof source[keys] === "object") {
          targetObj[keys] = this.getRequiredItem(source[keys]);
        } else {
          targetObj[keys] = source[keys];
        }
      });
      return targetObj;
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (error) {
        console.log(error);
      }
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
    async submit() {
      let dns = "";
      let id = localStorage.getItem("local_clusterID");
      let data = this.dbsource[0];
      let enterTip = this.$t("dataIn.enterTip");
      try {
        if (data.protocol && data.protocol.value) {
          dns += Object.is(data.protocol.value, "--")
            ? ""
            : data.protocol.value;
        }
        for (let key of Object.keys(data.options)) {
          if (
            Object.hasOwnProperty.call(data.options[key], "required") &&
            (data.options[key]["value"] == "" ||
              data.options[key]["value"] == undefined)
          ) {
            Message({
              type: "warning",
              message: `${enterTip} ${data.options[key].display} `,
            });
            return;
          }
        }
        if (this.tagName === "datasource") {
          if (data.authentication.value == "plain") {
            let userinfo = data.authentication.alternatives.filter(
              (item) => item.name == "plain"
            )[0];
            let username = window.encodeURIComponent(userinfo.username.value);
            let pwd = window.encodeURIComponent(userinfo.password.value);
            dns += `://`;
            if (this.handleEmptyValue(username)) {
              dns += `${username}`;
            }
            if (this.handleEmptyValue(pwd)) {
              dns += `:${pwd}`;
            }
          } else if (data.authentication.value == "token") {
            let userinfo = data.authentication.alternatives.filter(
              (item) => item.name == "token"
            )[0];
            let token = window.encodeURIComponent(userinfo.params[0].value);
            if (this.handleEmptyValue(token)) {
              dns += `${token}`;
            }
          }
          dns = dns.includes("://") ? dns : dns + "://";
          // if(this.handleEmptyValue(data.options.host.value)){
          dns += `@${data.options.host.value ? data.options.host.value : ""}`;
          // }
        } else {
          if (this.tagName == "influxdb") {
            this.changeHost(data.options.host.value);
            if (data.options.host.value && !this.isIP) {
              Message.warning(this.$t("datasource.iptip"));
              return;
            }
          }
          dns += `://${data.options.host.value ? data.options.host.value : ""}`;
        }

        if (data.options.port) {
          if (!this.isPort && this.tagName == "influxdb") {
            Message.warning(this.$t("datasource.porttip"));
            return;
          }
          dns +=
            (Object.is(data.options.port.value, null) ||
            !data.options.port.value
              ? ""
              : ":") +
            `${data.options.port.value ? data.options.port.value : ""}`;
        }

        dns += data.options.subject.value
          ? "/" + data.options.subject.value
          : "";
        let reg = /\s+/g;
        dns = dns.replace(reg, "").trim();
        let querystr = "";

        for (let index = 0; index < data.groups.length; index++) {
          //   for (let j = 0; j < data.groups[index].params.length; j++) {
          for (let g of Object.keys(data.groups[index].params)) {
            if (
              Object.hasOwnProperty.call(
                data.groups[index].params[g],
                "required"
              ) &&
              data.groups[index].params[g]["value"] == ""
            ) {
              Message({
                type: "warning",
                message: `${enterTip} ${data.groups[index].params[g].name} `,
              });
              return;
            } else {
              if (data.groups[index].params[g].value) {
                querystr +=
                  `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                  "&";
              }
            }
          }
          //   }
        }

        if (data.datasets) {
          for (
            let index = 0;
            index < data.datasets.categories.length;
            index++
          ) {
            // 判断必填项 多选时value为数组，单选时为字符串
            let target = data.datasets.categories[index].target;
            if (
              Object.hasOwnProperty.call(target, "required") &&
              target.required &&
              (target.value == null ||
                target.value == undefined ||
                target.value?.length == 0)
            ) {
              Message({
                type: "warning",
                message: `${enterTip} ${target.name} `,
              });
              return;
            } else {
              if (Array.isArray(target.value)) {
                if (target.value?.length > 0) {
                  let str = "";
                  for (let i = 0; i < target.value.length; i++) {
                    str += `${target.value[i]},`;
                  }
                  querystr += `${target.name}=${str.replace(/,$/g, "")}` + "&";
                }
              } else if (target.value != null || target.value != undefined) {
                querystr += `${target.name}=${target.value}` + "&";
              }
            }
          }
        }
        if (this.tagName == "influxdb") {
          let result = this.dbsource[0].authentication.alternatives.filter(
            (item) => item.name == this.dbsource[0].authentication.value
          );
          result[0].params.forEach((p) => {
            querystr += `${p.name}=${p.value}&`;
          });

          let requireTip = "";
          //influxdb需要校验authentication和task
          result.forEach((item) => {
            item.params.forEach((p) => {
              if (
                Object.hasOwnProperty.call(p, "required") &&
                p.value == null
              ) {
                requireTip += `${p.display}` + ",";
              }
            });
          });
          this.dbsource[0].groups.forEach((group) => {
            group.params.forEach((p) => {
              if (
                Object.hasOwnProperty.call(p, "required") &&
                p.value == null
              ) {
                requireTip += `${p.display}` + ",";
              }
            });
          });
          if (requireTip != "") {
            Message({
              type: "warning",
              message: `${enterTip} ${requireTip.replace(/,$/g, "")} `,
            });
            return;
          }
        }
        dns += querystr ? "?" + querystr.replace(/&$/g, "") : "";
        if (!this.dbname) {
          Message({
            type: "warning",
            message: `${enterTip} target database `,
          });
          return;
        }
        let apiParams = {
          from:
            "tmq" +
            (data.protocol
              ? Object.is(data.protocol.value, "--")
                ? ""
                : "+"
              : "") +
            dns,
          name: this.sourceName,
          to:
            "taos+" +
            localStorage.getItem("base_url") +
            (this.dbname ? "/" + this.dbname : ""),
          labels: [
            "type::datain",
            `cluster-id::${id}`,
            `user::${localStorage.getItem("username")}`,
          ],
        };
        if (this.$parent.agentID) {
          apiParams["via"] = this.$parent.agentID;
        }
        if (this.tagName === "datasource") {
          if (this.isEditable) {
            let result = await EditSource(apiParams, this.editId);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.toggleComponent("tmqtable");
          } else {
            let result = await AddSource(apiParams);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.toggleComponent("tmqtable");
          }
        } else {
          let piParams = {
            from:
              this.tagName == "influxdb"
                ? "influxdb" + dns
                : this.tagName + dns,
            name: this.sourceName,
            //   + (data.protocol?(Object.is(data.protocol.value, "--") ? "" : "+"):'') + dns,
            // name: localStorage.getItem("datainName"),
            to:
              "taos+" +
              localStorage.getItem("base_url") +
              (this.dbname ? "/" + this.dbname : ""),
            labels: [
              "type::datain",
              `cluster-id::${id}`,
              `user::${localStorage.getItem("username")}`,
            ],
          };
          if (this.$parent.agentID) {
            piParams["via"] = this.$parent.agentID;
          }
          if (this.isEditable) {
            let result = await EditSource(piParams, this.editId);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.toggleComponent("pitable");
          } else {
            let result = await AddSource(piParams);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            if (result && result.id) {
              this.$parent.toggleComponent("pitable");
              Message.success("Operation Successfully!");
            }
          }
        }
      } catch (err) {
        err.response &&
          err.response.data &&
          err.response.data.message &&
          Message.error(err.response.data.message);
      }
    },

    handleClick(tab, event) {
      this.isShowConfiguration = false;
      this.configurationdata = [];
      this.activeDataSet = {};
    },

    handleSelBtn() {
      this.isShowConfiguration = true;
    },

    addOption() {
      // "format": "{id}::{table}::{field}::{type}"
      let curData = this.configurationdata.filter(
        (item) => item.id === this.activeDataSet.id
      );
      let enterTip = this.$t("dataIn.enterTip");
      let format = curData[0].id;
      let options = curData[0].options;
      for (let i = 0; i < options.length; i++) {
        if (options[i].required && !options[i].value) {
          Message({
            type: "warning",
            message: `${enterTip} ${options[i].name}`,
          });
          return;
        }
        format += `::${options[i].value}`;
      }
      let categories = [];
      categories = this.dbsource[0].datasets.categories.map((cate) => {
        if (cate.category == this.activeDataSet.category) {
          if (Array.isArray(cate.target.value)) {
            cate.target.value.push(format);
            cate.target.value = Array.from(new Set(cate.target.value));
          } else {
            cate.target.value = format;
          }
        }
        return cate;
      });
    },
    handelDataSet(data) {
      this.activeDataSet = data;
      let categories = [];
      if (!Object.hasOwnProperty.call(data, "options")) {
        categories = this.dbsource[0].datasets.categories.map((cate) => {
          if (cate.category == data.category) {
            if (Array.isArray(cate.target.value)) {
              cate.target.value.push(data.id);
              cate.target.value = Array.from(new Set(cate.target.value));
            } else {
              cate.target.value = data.id;
            }
          }
          return cate;
        });
        this.dbsource[0].datasets.categories = categories;
      }
    },
    searchDatas: debounce(function (e) {
      try {
        let data = this.dbsource[0];
        let host = data.options.host.value ? data.options.host.value : "";
        let subject = data.options.subject.value
          ? "/" + data.options.subject.value
          : "";
        let enterTip = this.$t("dataIn.enterTip");
        if (!host) {
          Message({
            type: "warning",
            message: `${enterTip} ${data.options.host.display}`,
          });
          return;
        }
        if (!subject) {
          Message({
            type: "warning",
            message: `${enterTip} ${data.options.subject.display}`,
          });
          return;
        }
        let params = null;
        params = {
          from: `${this.tagName}://${host}${subject}`,
          categories: [this.activeName],
          pattern: e.target.value,
          offset: 0,
          limit: 10,
        };
        const viaObj = {
          via: this.$parent.agentID,
        };
        if (viaObj.via) {
          Object.assign(params, viaObj);
        }
        this.loading = true;
        getUaAndDaData(params)
          .then((res) => {
            this.loading = false;
            this.configurationdata = res;
          })
          .catch((err) => {
            Message({
              type: "error",
              message: err,
            });
          });
      } catch (error) {
        this.loading = false;
      }
    }, 100),
    changeBucket(value, name, choices) {
      if(name === 'bucket') {
        this.measurementList = this.bucketList.filter(item => item.id == value)
        // 清空 Measurements
        this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
          group.params.map((p) => {
            if (p.name === 'measurements') {
              p.value = []
            }
            return p
          });
          return group
        });
      }
    },
    getSchema() {
      this.btnLoading = true
      let dns = "";
      let id = localStorage.getItem("local_clusterID");
      let data = this.dbsource[0];
      let enterTip = this.$t("dataIn.enterTip");
      try {
        if (data.protocol && data.protocol.value) {
          dns += Object.is(data.protocol.value, "--")
            ? ""
            : data.protocol.value;
        }
        
        for (let key of Object.keys(data.options)) {
          if (
            Object.hasOwnProperty.call(data.options[key], "required") &&
            (data.options[key]["value"] == "" ||
              data.options[key]["value"] == undefined)
          ) {
            Message({
              type: "warning",
              message: `${enterTip} ${data.options[key].display} `,
            });
            return;
          }
        }
        // tagName === "datasource" 是tmq
        if (this.tagName === "datasource") {
          if (data.authentication.value == "plain") {
            let userinfo = data.authentication.alternatives.filter(
              (item) => item.name == "plain"
            )[0];
            let username = window.encodeURIComponent(userinfo.username.value);
            let pwd = window.encodeURIComponent(userinfo.password.value);
            dns += `://`;
            if (this.handleEmptyValue(username)) {
              dns += `${username}`;
            }
            if (this.handleEmptyValue(pwd)) {
              dns += `:${pwd}`;
            }
          } else if (data.authentication.value == "token") {
            let userinfo = data.authentication.alternatives.filter(
              (item) => item.name == "token"
            )[0];
            let token = window.encodeURIComponent(userinfo.params[0].value);
            if (this.handleEmptyValue(token)) {
              dns += `${token}`;
            }
          }
          dns = dns.includes("://") ? dns : dns + "://";
          // if(this.handleEmptyValue(data.options.host.value)){
          dns += `@${data.options.host.value ? data.options.host.value : ""}`;
          // }
        } else {
          if (this.tagName == "influxdb") {
            this.changeHost(data.options.host.value);
            if (data.options.host.value && !this.isIP) {
              Message.warning(this.$t("datasource.iptip"));
              return;
            }
          }
          dns += `://${data.options.host.value ? data.options.host.value : ""}`;
        }

        if (data.options.port) {
          if (!this.isPort && this.tagName == "influxdb") {
            Message.warning(this.$t("datasource.porttip"));
            return;
          }
          dns +=
            (Object.is(data.options.port.value, null) ||
            !data.options.port.value
              ? ""
              : ":") +
            `${data.options.port.value ? data.options.port.value : ""}`;
        }

        dns += data.options.subject.value
          ? "/" + data.options.subject.value
          : "";
        let reg = /\s+/g;
        dns = dns.replace(reg, "").trim();
        let querystr = "";
        // groups 是否需要校验必填
        for (let index = 0; index < data.groups.length; index++) {
          for (let g of Object.keys(data.groups[index].params)) {
              if (data.groups[index].params[g].value) {
                querystr +=
                  `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                  "&";
              }
          }
        }
        // 
        if (this.tagName == "influxdb") {
          let result = this.dbsource[0].authentication.alternatives.filter(
            (item) => item.name == this.dbsource[0].authentication.value
          );
          result[0].params.forEach((p) => {
            querystr += `${p.name}=${p.value}&`;
          });

          let requireTip = "";
          //influxdb需要校验authentication和task
          result.forEach((item) => {
            item.params.forEach((p) => {
              if (
                Object.hasOwnProperty.call(p, "required") &&
                p.value == null
              ) {
                requireTip += `${p.display}` + ",";
              }
            });
          });
          this.dbsource[0].groups.forEach((group) => {
            group.params.forEach((p) => {
              if (
                Object.hasOwnProperty.call(p, "required") &&
                p.value == null
              ) {
                requireTip += `${p.display}` + ",";
              }
            });
          });
        }
        dns += querystr ? "?" + querystr.replace(/&$/g, "") : "";
        
        let apiParams = {
          from:
            "tmq" +
            (data.protocol
              ? Object.is(data.protocol.value, "--")
                ? ""
                : "+"
              : "") +
            dns,
          name: this.sourceName,
          categories:["nodes"],
          pattern: "api",
          offset:0,
          limit:10,
        };
        if (this.$parent.agentID) {
          apiParams["via"] = this.$parent.agentID;
        }
        if (this.tagName === "datasource") {
          getUaAndDaData(apiParams)
          .then((res) => {
            console.log('apiParams',res);
          })
          .catch((err) => {
            Message({
              type: "error",
              message: err,
            });
          });         
        } else {
          let piParams = {
            from:
              this.tagName == "influxdb"
                ? "influxdb" + dns
                : this.tagName + dns,
            categories:["nodes"],
            pattern: "api",
            offset:0,
            limit:10,
            };
          if (this.$parent.agentID) {
            piParams["via"] = this.$parent.agentID;
          }
            getUaAndDaData(piParams)
            .then((res) => {
              this.bucketList = res[0].id !== '' && Object.keys(JSON.parse(res[0].id)).map(item => {
                return {id: item, children: JSON.parse(res[0].id)[item][0]}
              }) 
              this.btnLoading = false
            })
            .catch((err) => {
              console.log('err',err);
              this.btnLoading = false
              Message({
                type: "error",
                message: err,
              });
            });
        }
      } catch (err) {
        this.btnLoading = false
        console.log('err');
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.source-ui {
  padding-left: 20px;
  justify-content: space-around;
  //   padding-right: 300px;
  display: flex;
  :deep {
    .el-input__inner {
      border: none !important;
      box-shadow: inset 0 0 0 1px rgb(190, 188, 188);
    }
  }
  .label-value {
    display: flex;
    flex-direction: column;
    // max-width: 500px;
    color: #acaab2;
    white-space: pre-wrap;
  }
  .left-ui.readable {
    position: relative;
    &::before {
      content: "";
      display: block;
      background: #f2f6fc40;
      position: absolute;
      top:0;
      left: 0;
      right: 0;
      bottom: 0;
      z-index:100;
    }
  }

  .left-ui {
    min-width: 800px;
    .description {
      max-width: 500px;
      overflow: auto;
    }
    .target-db-name {
      display: flex;
      flex-direction: column;
      flex: 1;
      .desc {
        color: red;
        display: block;
        margin-top: 8px;
      }
    }
    .source-name {
      border: 1px solid #e3e4e6;
      padding: 15px;
      border-radius: 12px;
      margin-bottom: 20px;
      .name {
        display: flex;
        align-items: center;
        ::v-deep .el-input {
          flex: 1;
        }
      }
    }
    section:not(:first-child) {
      border: 1px solid #ececef;
      margin-bottom: 20px;
      border-radius: 12px;
      padding: 15px;
      // border-bottom: 1px solid #ececef;
    }
    .block-title {
      span {
        font-size: 16px;
        color: #4259ce;
        font-weight: 600;
      }
    }
    .label {
      font-size: 14px;
      color: #4259ce;
      align-items: center;
      width: 200px;
      display: block;
    }
    .no-label {
      align-items: center;
      width: 8px;
    }
    .label.required,
    .no-label.required {
      position: relative;
      &::before {
        content: "*";
        // position: absolute;
        color: red;
        font-size: 14px;
        line-height: 25px;
        left: -10px;
      }
    }
    .header {
      margin-bottom: 20px;
      h1 {
        font-size: 20px;
        font-weight: 700;
        line-height: 30px;
        color: #4259ce;
        margin-bottom: 10px;
      }
      h3 {
        font-size: 14px;
        color: #4259ce;
      }
    }
    .basics {
      display: flex;
      flex-direction: column;

      :deep {
        .el-input__inner {
          flex: auto;
          // width: 660px;
        }
        .el-select {
          width: 100%;
        }
      }
      div,
      p {
        white-space: pre-wrap;
        display: flex;
        align-items: baseline;
        // margin-bottom: 8px;
        flex: 1;
      }
      .first {
        display: flex;
        flex-direction: column;
        // grid-template-columns: 1fr 1fr;
        // column-gap: 10px;
      }
    }
    .groups {
      div {
        display: flex;
        white-space: nowrap;
        align-items: baseline;
        margin-bottom: 8px;
      }
      .select-with-btn {
        width: 100%;
        margin-bottom: 0 !important;
      }
      .label-value {
        flex: auto;
      }
      .el-input {
        flex: 1;
        display: flex;
        width: 100%;
      }
      .el-select {
        margin-left: 0px !important;
        width: 100%;
      }
    }
    .choose-db {
      display: flex;
      align-items: center;
      .el-select {
        flex: auto;
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
    .authentication {
      .authen-content {
        margin-top: 15px;
      }
      .authen-details {
        margin-top: 15px;
      }
      .plain {
        .plain-item {
          display: flex;
          margin-bottom: 10px;
          align-items: baseline;
        }
      }
    }
  }
  .right-ui {
    margin-left: 20px;
    padding-top: 50px;
    :deep {
      .v-note-panel {
        border-radius: 12px;
      }
    }
  }
  .description {
    display: initial !important;
    color: #acaab2;
    margin-bottom: 0px !important;
  }
  :deep {
    .el-input-number__increase,
    .el-input-number__decrease {
      height: 38px;
      display: flex;
      justify-content: center;
      align-items: center;
    }
  }
  .target {
    display: flex;
    margin-top: 24px;
    .el-input {
      width: 50%;
      margin-right: 24px;
    }
    .el-select {
      width: 50%;
      margin-right: 24px;
    }
  }
  .configuration {
    > div {
      display: flex;
      margin-top: 16px;
    }
    margin-top: 24px;
    margin-left: 8px;
    .el-input {
      width: 50%;
    }
    .searchList {
      width: 50%;
      height: 210px;
      border: 1px solid #dcdfe6;
      overflow-y: auto;
      > div {
        border-bottom: 1px solid #dcdfe6;
        line-height: 30px;
      }
      .actived {
        color: #4259ce;
        border-color: #c6cdf0;
        background-color: #eceefa;
      }
      :hover {
        cursor: pointer;
        color: #4259ce;
        border-color: #c6cdf0;
        background-color: #eceefa;
      }
    }
    .options-wrap {
      height: 210px;
      margin-left: 24px;
      border: 1px solid #dcdfe6;
      padding: 16px 8px;
      flex: 1;
      .option-list {
        overflow-y: auto;
        height: 150px;
        padding-left: 10px;
        .option-item {
          display: flex;
          white-space: nowrap;
          align-items: baseline;
          margin-bottom: 8px;
          .label {
            font-size: 14px;
            color: #4259ce;
            align-items: center;
            width: 100px;
            display: block;
          }
          .el-input {
            flex: 1;
          }
        }
      }
      :last-child {
        display: flex;
        justify-content: flex-end;
      }
    }
  }
}
</style>
