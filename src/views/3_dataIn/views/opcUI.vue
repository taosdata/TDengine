<template>
  <div class="source-ui">
    <div class="left-ui">
      <section class="header">
        <h1>{{ dbsource[0].name ? dbsource[0].name : "" }}</h1>
      </section>

      <section class="basics">
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
          <div
            style="width: 100%"
            v-if="
              JSON.stringify(dbsource[0].options) !== '{}' &&
              JSON.stringify(dbsource[0].options.endpoint) !== '{}'
            "
          >
            <span
              :class="[
                'label',
                dbsource[0].options.endpoint &&
                dbsource[0].options.endpoint.required
                  ? 'required'
                  : '',
              ]"
              >{{
                dbsource[0].options.endpoint
                  ? dbsource[0].options.endpoint.display
                  : ""
              }}</span
            >
            <div class="label-value" v-if="dbsource[0].options.endpoint">
              <el-input
                style="margin-bottom: 8px"
                v-model="dbsource[0].options.endpoint.value"
                :placeholder="
                  dbsource[0].options.endpoint
                    ? dbsource[0].options.endpoint.placeholder
                    : ''
                "
              ></el-input>
              <div
                v-html="transforHtml(dbsource[0].options.endpoint.description)"
                class="description"
              ></div>
            </div>
          </div>
        </div>
      </section>
      <section
        class="authentication"
        v-if="dbsource[0].authentication?.display"
      >
        <div>
          <div class="block-title">
            <span>{{ dbsource[0].authentication?.display }}</span>
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
                      <span
                        :class="[
                          'label',
                          at.username.required ? 'required' : '',
                        ]"
                        >{{ at.username.display }}</span
                      >
                      <div style="flex: 1">
                        <el-input
                          style="margin-bottom: 8px"
                          v-model="at.username.value"
                        ></el-input>
                        <p
                          class="description"
                          v-html="transforHtml(at.username.description)"
                        ></p>
                      </div>
                    </div>

                    <div class="plain-item">
                      <span
                        :class="[
                          'label',
                          at.password.required ? 'required' : '',
                        ]"
                        >{{ at.password.display }}</span
                      >
                      <div style="flex: 1">
                        <el-input
                          type="password"
                          style="margin-bottom: 8px"
                          v-model="at.password.value"
                        ></el-input>
                        <p
                          class="description"
                          v-html="transforHtml(at.password.description)"
                        ></p>
                      </div>
                    </div>
                  </div>
                </template>
                <div
                  v-else
                  v-for="(p, index) in at.params"
                  :key="index"
                  :style="textareas.includes(p.name) ? styleareaobj : styleobj"
                >
                  <span
                    :class="['label', p.required ? 'required' : '']"
                    :style="
                      textareas.includes(p.name)
                        ? { 'padding-top': '10px!important' }
                        : {}
                    "
                    >{{ p.display }}</span
                  >

                  <div style="flex: 1">
                    <template v-if="p.hint && p.hint.choices">
                      <el-select
                        v-model="p.value"
                        placeholder=""
                        style="
                          margin-left: 0px;
                          width: 100%;
                          margin-bottom: 8px;
                        "
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
                          : textareas.includes(p.name)
                          ? 'textarea'
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
                      placeholder=""
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
                    >Select</el-button
                  >
                </div>
                <div class="configuration" v-if="isShowConfiguration">
                  <el-input
                    placeholder="Regex Pattern Input"
                    v-model="p.value"
                    :disable="p.target.selectable"
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
                            >Add</el-button
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
      <template v-for="(item, gind) in dbsource[0].groups">
        <section :class="['groups', item.name]" :key="gind">
          <div style="flex-direction: column; align-items: baseline">
            <div class="block-title">
              <span>{{ item.name }}</span>
            </div>
            <div
              class="description"
              v-html="transforHtml(item.description)"
            ></div>
          </div>
          <template
            v-if="
              item.hasOwnProperty('collapsible') && item.name.includes('SSL')
            "
          >
            <div
              class="switch-ssl"
              style="display: flex; align-items: flex-start"
            >
              <span style="color: #4259ce; margin-right: 10px">SSL/TSL</span>
              <!-- <template v-if="item.hasOwnProperty('collapsed')">
                <el-switch v-model="item.collapsed"> </el-switch>
              </template> -->
              <el-switch v-model="item.collapsed"> </el-switch>
            </div>
          </template>
          <template v-if="item.collapsed && item.name.includes('SSL')">
            <template v-for="p in item.params">
              <div
                :key="p.name"
                v-if="item.collapsed && item.name.includes('SSL')"
                class="ssl"
              >
                <span :class="['label', p.required ? 'required' : '']">
                  {{ p.display ? p.display : p.name }}
                </span>
                <div class="label-value">
                  <template
                    v-if="
                      p.hint === 'str' ||
                      p.hint === 'timeout' ||
                      p.hint.type == 'file'
                    "
                  >
                    <el-input
                      v-model="p.value"
                      :placeholder="p.placeholder ? p.placeholder : ''"
                      :type="
                        p.hint.type && p.hint.type == 'file'
                          ? 'textarea'
                          : 'text'
                      "
                    ></el-input>
                  </template>
                  <template v-if="p.hint.type && p.hint.type === 'str'">
                    <template v-if="p.hint.choices">
                      <el-select v-model="p.value" placeholder="">
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
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input>
                  </template>
                  <template
                    v-if="
                      (p.hint === 'bool' || p.hint.type === 'bool') &&
                      p.name == 'clean_session'
                    "
                  >
                    <el-radio-group v-model="p.value" v-if="p.choices">
                      <el-radio v-for="c in p.choices" :key="c" :label="c">
                        {{ c }}
                      </el-radio>
                    </el-radio-group>
                    <template v-else>
                      <el-checkbox
                        v-model="p.value"
                        :true-label="true"
                        :false-label="false"
                      ></el-checkbox>
                    </template>
                  </template>
                  <template v-else-if="p.hint.type && p.hint.type === 'bool'">
                    <p-three-checkbox :data="checkboxData" v-model="p.value" />
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
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input-number>
                  </template>
                  <div
                    v-html="transforHtml(p.description)"
                    class="description"
                  ></div>
                </div>
              </div>
            </template>
          </template>
          <template v-else>
            <template v-for="(p, pind) in item.params">
              <div :key="p.name" v-if="!item.name.includes('SSL')">
                <span :class="['label', p.required ? 'required' : '']">
                  {{ p.display ? p.display : p.name }}
                </span>
                <div class="label-value">
                  <template v-if="p.hint === 'str' || p.hint === 'timeout'">
                    <el-input
                      v-model="p.value"
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input>
                  </template>
                  <template v-if="p.hint.type && p.hint.type === 'str'">
                    <template v-if="p.hint.choices">
                      <el-select
                        v-model="p.value"
                        :placeholder="p.placeholder ? p.placeholder : ''"
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
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input>
                  </template>
                  <template
                    v-if="
                      (p.hint === 'bool' || p.hint.type === 'bool') &&
                      p.name == 'clean_session'
                    "
                  >
                    <el-radio-group v-model="p.value" v-if="p.choices">
                      <el-radio v-for="c in p.choices" :key="c" :label="c">
                        {{ c }}
                      </el-radio>
                    </el-radio-group>
                    <template v-else>
                      <el-checkbox
                        v-model="p.value"
                        true-label="true"
                        false-label="false"
                      ></el-checkbox>
                    </template>
                  </template>
                  <template v-else-if="p.hint.type && p.hint.type === 'bool'">
                    <p-three-checkbox :data="checkboxData" v-model="p.value" />
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
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input-number>
                  </template>
                  <div
                    v-html="transforHtml(p.description)"
                    class="description"
                  ></div>
                </div>
              </div>
              <template v-if="p.name == 'opc_table_config'">
                <div :key="pind">
                  <opcConnector
                    :opcConfig="opcConfig"
                    :isEditable="isEditable"
                    :echoData="echoData"
                    @changeEchoData="changeEchoData"
                    ref="opcsingleton"
                  ></opcConnector>
                </div>
              </template>
            </template>
          </template>
        </section>
      </template>

      <!--未分组显示根节点下的params，显示方式和groups一样-->
      <section class="ungrounded" v-if="dbsource[0].params"></section>
      <section v-if="tagName == 'mqtt'" class="mqtt-config">
        <div class="header">
          <div class="block-title">
            <span>{{ dbsource[0].parser?.display }}</span>
          </div>
          <div
            class="description"
            v-html="transforHtml(dbsource[0].parser.description)"
          ></div>
        </div>

        <div class="parser-config">
          <MqttConnector
            :connectorData="constMqttparser"
            :fields="constmqttCols"
            ref="mqtt"
            :isEditable="isEditable"
          ></MqttConnector>
        </div>
      </section>
      <section class="choose-db">
        <span class="label required">{{ $t("datasource.targetdb") }}</span>
        <el-select v-model="dbname" placeholder="">
          <el-option
            v-for="db in dblist"
            :key="db['node-key']"
            :label="db.name"
            :value="db.name"
          ></el-option>
        </el-select>
      </section>
      <section class="bottom">
        <el-button type="primary" @click="submit" :disabled="disable"
          >Submit</el-button
        >
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
import { Message } from "element-ui";
import marked from "marked";
import { decrypt, debounce, deepClone } from "@/utils/index";
import PThreeCheckbox from "../components/pThreeCheckbox.vue";
import MqttConnector from "../components/mqttConnector.vue";
import opcConnector from "../components/opcConnector.vue";
export default {
  name: "DbSourceUI",
  components: {
    "p-three-checkbox": PThreeCheckbox,
    MqttConnector,
    opcConnector,
  },
  props: {
    echoData: {
      type: Array,
      default: () => {
        return [];
      },
    },
    opcConfig: {
      type: Object,
      default: () => {
        return null;
      },
    },
    constMqttparser: {
      type: Object,
      default: () => {
        return null;
      },
    },
    mqttParser: {
      type: Object,
      default: () => {
        return null;
      },
    },
    tagName: {
      type: String,
      default: "opcua",
    },
    protocol: {
      type: String,
      default: "ua",
    },
    dbsource: {
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
    return {
      openSSL: false,
      constmqttCols: [],
      textareas: ["ca", "cert", "cert_key", "certificate"],
      styleobj: {
        width: "100%",
        display: "flex",
        "align-items": "baseline",
        "margin-bottom": "8px",
      },
      styleareaobj: {
        width: "100%",
        display: "flex",
        "margin-bottom": "8px",
      },
      payloadVal: "",
      mqttpayload: ["json"],
      decryptPwd: "", //解密的密码
      // dbsource,
      disable: false,
      address: "",
      port: "",
      username: "",
      password: "",
      subject: "",
      radio: "",
      dblist: [],
      dbname: "",
      isShowConfiguration: false,
      loading: false,
      configurationdata: [],
      activeDataSet: {},
      activeName: "",
      checkboxData: {
        label: "",
        disabled: false,
      },
      // dbsource: [],
    };
  },
  created() {
    this.getDatabases();
    if (this.isEditable) {
      this.dbname = this.dbName;
      if (this.tagName == "mqtt") {
        this.payloadVal = "json";
      }
    }
  },
  mounted() {
    if (this.tagName == "mqtt") {
      this.constmqttCols = this.dbsource[0].parser.fields;
    }
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
    //opc需要存入库的字段
    changeEchoData(arr) {
      this.$parent.echoData = deepClone(arr);
    },
    selectPayload(val) {
      this.payloadVal = val;
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
              message:
                this.$t("datasource.msg") +
                ":" +
                `${data.options[key].display} `,
            });
            return;
          }
        }
        this.decryptPwd = decrypt(localStorage.getItem("pwd"));
        if (data.authentication.value == "plain") {
          if (
            data.authentication.alternatives[this.tagName == "mqtt" ? 0 : 1]
              .username.value
          ) {
            dns += `://${
              data.authentication.alternatives[this.tagName == "mqtt" ? 0 : 1]
                .username.value
            }`;
          } else {
            dns += `://`;
          }
          if (
            data.authentication.alternatives[this.tagName == "mqtt" ? 0 : 1]
              .password.value
          ) {
            dns += `:${
              data.authentication.alternatives[this.tagName == "mqtt" ? 0 : 1]
                .password.value
            }`;
          }
          dns += `@`;
        } else {
          dns += `://`;
        }
        if (
          data.options.endpoint &&
          JSON.stringify(data.options.endpoint) !== "{}"
        ) {
          dns += `${
            data.options.endpoint.value ? data.options.endpoint.value : "/"
          }`;
        }
        //  else {
        //   dns += `:///`;
        // }
        // dns += data.options.subject.value
        //   ? "/" + data.options.subject.value
        //   : "";
        let reg = /\s+/g;
        dns = dns.replace(reg, "").trim();
        let querystr = "";
        for (let index = 0; index < data.groups.length; index++) {
          for (let g = 0; g < data.groups[index].params.length; g++) {
            if (
              Object.hasOwnProperty.call(
                data.groups[index].params[g],
                "required"
              ) &&
              (data.groups[index].params[g]["value"] == undefined ||
                data.groups[index].params[g]["value"] == "")
            ) {
              if (this.tagName == "mqtt") {

                if (data.groups[index].collapsed) {
                  Message({
                    type: "warning",
                    message:
                      this.$t("datasource.msg") +
                      ":" +
                      `${data.groups[index].params[g].display} `,
                  });
                  return;
                }
                if (data.groups[index].params[g].name == "topics") {
                  Message({
                    type: "warning",
                    message:
                      this.$t("datasource.msg") +
                      ":" +
                      `${data.groups[index].params[g].display} `,
                  });
                  return;
                }
              } else {
                if (this.tagName.includes("opc")) {
                  this.$refs.opcsingleton[0].submit();
                  if (this.$refs.opcsingleton[0].isReject) {
                    Message({
                      type: "warning",
                      message:
                        this.$t("datasource.msg") +
                        ":" +
                        `${data.groups[index].params[g].display} `,
                    });
                    return;
                  }
                } else {
                  Message({
                    type: "warning",
                    message:
                      this.$t("datasource.msg") +
                      ":" +
                      `${data.groups[index].params[g].display} `,
                  });
                  return;
                }
              }
            } else {
              if (data.groups[index].params[g].value) {
                if (data.groups[index].params[g].name === "use_received_time") {
                  if (data.groups[index].params[g].value !== 0) {
                    let value = data.groups[index].params[g].value === 1;
                    querystr +=
                      `${data.groups[index].params[g].name}=${value}` + "&";
                  }
                } else {
                  if (this.tagName == "mqtt") {
                    if (
                      !Object.hasOwnProperty.call(
                        data.groups[index],
                        "collapsed"
                      ) ||
                      data.groups[index].collapsed
                    ) {
                      querystr +=
                        `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                        "&";
                    }
                  } else {
                    if (
                      data.groups[index].params[g].name != "opc_table_config"
                    ) {
                      if (data.groups[index].params[g].name == "debug") {
                        querystr +=
                          `${data.groups[index].params[g].name}=${
                            data.groups[index].params[g].value == 1
                              ? true
                              : false
                          }` + "&";
                      } else {
                        querystr +=
                          `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                          "&";
                      }
                    }

                    // }
                  }
                }
              }
            }
          }
        }
        console.log(querystr, "querystr");
        if (data.authentication.value == "certificates") {
          data.authentication.alternatives[2].params.forEach((val) => {
            querystr += val.value ? `${val.name}=${val.value}&` : "";
          });
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
        if (querystr) {
          dns += querystr ? "?" + querystr.replace(/&$/g, "") : "";
        }

        if (!this.dbname) {
          Message({
            type: "warning",
            message: `${enterTip}  ` + this.$t("datasource.targetdb"),
          });
          return;
        }
        if (this.tagName == "mqtt") {
          this.$refs.mqtt.submit();
          console.log(this.$refs.mqtt, "mqtt");
          if (this.$refs.mqtt) {
            if (this.$refs.mqtt.showSuperTip) {
              Message({
                type: "warning",
                message: this.$t("datasource.bothtagsuper"),
              });
              return;
            }
            if (this.$refs.mqtt.disable || this.$refs.mqtt.nameisnull) {
              Message({
                type: "warning",
                message: this.$t("datasource.mqttparsertip"),
              });
              return;
            }
          }
          let oldparser = this.$store.state.app.mqttParser;
          let columns = oldparser.model.columns;
          if (columns.includes(this.$refs.mqtt.defaultSelect)) {
            columns.map((item, ind) => {
              if (item == this.$refs.mqtt.defaultSelect) {
                columns.unshift(columns.splice(ind, 1)[0]);
              }
            });
          }
          this.$store.commit("app/SET_MQTT_PARSER", this.constMqttparser);
        }

        if (this.tagName.includes("opc")) {
          let oldData = this.$store.state.app.opcConfig;
          let columnCons = oldData.column_configs.filter((item) =>
            this.$parent.echoData.includes(item.column_name)
          );
          this.$store.commit("app/SET_OPC_CONFIG", {
            column_configs: columnCons,
            stable_prefix: oldData.stable_prefix,
          });
          let saveConf = {
            column_configs: columnCons,
            stable_prefix: oldData.stable_prefix,
          };
          dns += "&opc_table_config=" + JSON.stringify(saveConf);
        }
        console.log(dns, querystr, "mqtt调用接口前的参数拼接");
        let piParams = {
          from:
            (this.tagName == "mqtt" ? "mqtt" : "opc" + this.protocol) +
            // (data.protocol
            //   ? Object.is(data.protocol.value, "--")
            //     ? ""
            //     : "+"
            //   : "") +
            dns,
          name: localStorage.getItem("datainName"),
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
        if (this.tagName == "mqtt") {
          piParams["parser"] = this.$store.state.app.mqttParser;
        }
        if (this.$parent.agentID) {
          piParams["via"] = this.$parent.agentID;
        }
        if (this.isEditable) {
          let result = await EditSource(piParams, this.editId);
          if (result.message) {
            Message.error(result.message);
            return;
          }
          this.$parent.toggleComponent("opctable", this.protocol);
        } else {
          let result = await AddSource(piParams);
          if (result.message) {
            Message.error(result.message);
            return;
          }
          if (result && result.id) {
            this.$parent.toggleComponent("opctable", "");
            Message.success(this.$t("datasource.successtip"));
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
      console.log('搜索');
      try {
        let data = this.dbsource[0];
        let endpoint = data.options.endpoint.value;
        let enterTip = this.$t("dataIn.enterTip");
        if (!endpoint) {
          Message({
            type: "warning",
            message: `${enterTip} ${data.options.endpoint.display}`,
          });
          return;
        }

        let dns = "";
        let querystr = "";
        if (data.authentication.value == "certificates") {
          data.authentication.alternatives[2].params.forEach((val) => {
            querystr += val.value ? `${val.name}=${val.value}&` : "";
          });
        }
        if (data.authentication.value == "plain") {
          if (data.authentication.alternatives[1].username.value) {
            dns += `://${data.authentication.alternatives[1].username.value}`;
          }
          if (data.authentication.alternatives[1].password.value) {
            dns += `:${data.authentication.alternatives[1].password.value}`;
          }
          dns += `@`;
        } else {
          dns += `://`;
        }
        if (
          data.options.endpoint &&
          JSON.stringify(data.options.endpoint) !== "{}"
        ) {
          dns += `${
            data.options.endpoint.value ? data.options.endpoint.value : "/"
          }`;
        }
        dns += querystr ? "?" + querystr.replace(/&$/g, "") : "";

        let params = null;
        params = {
          from: `opc${this.protocol}${dns}`,
          categories: [this.activeName],
          pattern: e.target.value,
          offset: 0,
          limit: 10,
        };
        if (this.$parent.agentID) {
          const viaObj = {
            via: this.$parent.agentID,
          };
          if (viaObj.via) {
            Object.assign(params, viaObj);
          }
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
  },
};
</script>
<style>
.el-select-dropdown__item {
  font-family: "element-icons" !important;
  font-weight: 500;
}
</style>
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
    .el-textarea__inner {
      min-height: 40px !important;
      height: 40px;
    }
  }
  .label-value {
    display: flex;
    flex-direction: column;
    // max-width: 500px;
    color: #acaab2;
    white-space: pre-wrap;
  }
  .left-ui {
    overflow: auto;
    min-width: 800px;
    .description {
      max-width: 500px;
      overflow: auto;
    }
    section:not(:first-child) {
      border: 1px solid #e3e4e6;
      margin-bottom: 20px;
      border-radius: 12px;
      padding: 15px;
    }
    .block-title {
      margin-bottom: 10px;
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
    .label.required, .no-label.required {
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
      .label-value {
        flex: auto;
      }
      .el-input {
        flex: 1;
        display: flex;
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

  .mqtt-fields {
    margin-bottom: 25px;
    li {
      display: flex;
      margin-bottom: 8px;
      margin-top: 15px;
      align-items: baseline;
      .mqtt-field {
        flex: 1;
        width: 100%;
        .el-select {
          width: 100%;
          margin-bottom: 8px;
        }
      }
    }
  }
}
</style>
