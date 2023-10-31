<template>
  <div class="source-ui">
    <div
      :class="[
        'left-ui',
        // this.$parent.currentTaskStatus == 'running' && !this.$parent.isCopyable
        isShowEditBtn ? 'readable' : '',
      ]"
    >
      <section>
        <DataTarget></DataTarget>
      </section>

      <section class="basics">
        <div class="block-title">
          <span>{{ $t("dataIn.connectionConfiguration") }}</span>
        </div>
        <div class="protocol" v-if="dbsource[0].protocol">
          <span class="label">{{ dbsource[0].protocol.display }}</span>
          <div class="label-value">
            <el-select
              v-model="dbsource[0].protocol.value"
              placeholder=""
              style="margin-bottom: 8px"
              size="small"
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
          <!-- 根节点下的 params 参数 -->
          <div
            style="width: 100%"
            v-if="
              dbsource[0].params &&
              dbsource[0]?.params[0] &&
              JSON.stringify(dbsource[0]?.params[0]) !== '{}'
            "
          >
            <span
              :class="[
                'label',
                dbsource[0].params[0].required ? 'required' : '',
              ]"
              >{{ dbsource[0].params[0].display }}</span
            >
            <div class="label-value">
              <el-select
                v-model="dbsource[0].params[0].value"
                placeholder=""
                size="small"
                style="margin-bottom: 8px"
                @change="changeSystemConfiguration"
              >
                <el-option
                  v-for="c in dbsource[0].params[0].hint.choices"
                  :key="c"
                  :label="c"
                  :value="c"
                ></el-option>
              </el-select>
              <div
                v-html="transforHtml(dbsource[0].params[0].description)"
                class="description"
              ></div>
            </div>
          </div>
          <div style="width: 100%">
            <span
              :class="[
                'label',
                dbsource[0].options?.endpoint?.required ? 'required' : '',
              ]"
              >{{ dbsource[0].options?.endpoint?.display }}</span
            >
            <div class="label-value" v-if="dbsource[0]?.options?.endpoint">
              <el-input
                size="small"
                v-model="dbsource[0].options.endpoint.value"
                @change="changeHost(dbsource[0].options?.endpoint?.display)"
                :placeholder="dbsource[0].options?.endpoint?.placeholder"
                style="margin-bottom: 8px"
              ></el-input>
              <div
                v-html="
                  transforHtml(dbsource[0].options?.endpoint?.description)
                "
                class="description"
              ></div>
            </div>
          </div>
          <div style="width: 100%">
            <span
              :class="[
                'label',
                dbsource[0].options?.host?.required ? 'required' : '',
              ]"
              >{{ dbsource[0].options?.host?.display }}</span
            >

            <div class="label-value" v-if="dbsource[0]?.options?.host">
              <el-input
                size="small"
                v-model="dbsource[0].options.host.value"
                @change="changeHost(dbsource[0].options?.host?.display)"
                :placeholder="dbsource[0].options?.host?.placeholder"
                style="margin-bottom: 8px"
              ></el-input>
              <div
                v-html="transforHtml(dbsource[0].options?.host?.description)"
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
                size="small"
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
        <!-- 根节点下的 params 参数 -->
        <div
          style="width: 100%"
          v-if="dbsource[0].params && isPiDataArchiveAll"
        >
          <span
            :class="['label', dbsource[0].params[1].required ? 'required' : '']"
            >{{ dbsource[0].params[1].display }}</span
          >
          <div class="label-value">
            <el-input
              size="small"
              :placeholder="dbsource[0].params[1].placeholder"
              v-model="dbsource[0].params[1].value"
              style="margin-bottom: 8px"
            ></el-input>
            <div
              v-html="transforHtml(dbsource[0].params[1].description)"
              class="description"
            ></div>
          </div>
        </div>
        <!-- pi 的 AF Database Name 需要根据 System Configuration 值确认-->
        <div
          style="width: 100%"
          v-if="
            JSON.stringify(dbsource[0].options.subject) !== '{}' &&
            isPiDataArchiveAll
          "
        >
          <span
            :class="[
              'label',
              dbsource[0].options?.subject?.required ? 'required' : '',
            ]"
            >{{ dbsource[0].options?.subject?.display }}</span
          >
          <div class="label-value" v-if="dbsource[0]?.options?.subject">
            <el-input
              size="small"
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
                          size="small"
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
                          size="small"
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
                        size="small"
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
                    <template v-else-if="p.hint && p.hint.type == 'bool'">
                      <el-switch
                        v-model="p.value"
                        :active-value="'true'"
                        :inactive-value="'false'"
                      >
                      </el-switch>
                    </template>
                    <el-input
                      v-else
                      size="small"
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
        </div>
      </section>
      <section>
        <el-collapse v-model="activeCollapse" accordion>
          <el-collapse-item name='one'>
            <template slot="title">
              <el-button
                :loading="checkLoading"
                type="primary"
                size="small"
                @click.capture.stop="clickCheckBtn"
                >{{ $t("dataIn.check") }}
              </el-button>
            </template>
            <Result
              v-show="JSON.stringify(checkResult) !== '{}'"
              :result="checkResult"
            /> 
          </el-collapse-item>
        </el-collapse>
      </section>
      <section
        class="dataset"
        v-if="
          dbsource[0].datasets &&
          (dbsource[0].datasets.display || dbsource[0].datasets.name)
        "
      >
        <div>
          <div class="block-title">
            <span>{{
              dbsource[0].datasets.display || dbsource[0].datasets.name
            }}</span>
          </div>
          <div
            class="description"
            v-html="transforHtml(dbsource[0].datasets.description)"
          ></div>
        </div>
        <template>
          <el-tabs
            v-model="activeName"
            @tab-click="handleClick"
            class="pi-tab-item"
            style="margin-top: 8px"
          >
            <div class="upload-flex">
              <el-radio-group v-model="activeRadio">
                <el-radio label="select_file">{{ $t("uploadcsv") }}</el-radio>
                <el-radio label="all_points">{{
                  activeName === "point_file"
                    ? $t("allPoints")
                    : $t("allTemplate")
                }}</el-radio>
              </el-radio-group>
            </div>
            <el-tab-pane
              v-for="(p, pind) in dbsource[0].datasets.params"
              :label="p.display"
              :name="p.name"
              :key="p.name"
              lazy
              :disabled="
                !['point_file'].includes(p.name) && !isPiDataArchiveAll
              "
            >
              <div
                :key="pind"
                style="margin-bottom: 0px"
                v-if="activeRadio == 'select_file'"
              >
                <div class="upload-flex">
                  <el-upload
                    class="upload-dataset"
                    ref="upload"
                    accept=".csv"
                    :data="uploadData"
                    :action="uploadUrl"
                    :on-success="
                      (response, file, fileList) =>
                        handleSuccess(response, file, fileList, p.name)
                    "
                    :file-list="p.fileList"
                    :auto-upload="true"
                    :on-remove="() => handleRemove(p.name)"
                    :on-preview="() => handlePreview(p.value)"
                  >
                    <el-button
                      slot="trigger"
                      size="small"
                      type="primary"
                      style="margin-right: 20px"
                      >{{ $t("datasource.selectfile") }}
                    </el-button>
                  </el-upload>
                  <template v-if="activeName === 'point_file'">
                    <el-tooltip
                      class="item"
                      effect="light"
                      :content="$t('downloadTemplateTip')"
                      placement="top-start"
                    >
                      <a href="/Points.csv" download style="padding-left: 16px">
                        <i
                          class="el-icon-download"
                          style="padding-right: 2px"
                        ></i
                        >{{ $t("downloadTemplate") }}</a
                      >
                    </el-tooltip>
                    <el-tooltip
                      class="item"
                      effect="light"
                      :content="$t('downloadPiPointTip')"
                      placement="top-start"
                    >
                      <el-button
                        type="text"
                        style="padding-left: 16px"
                        @click="searchDatas($t('downloadPiPoint'))"
                        :disabled="loading"
                      >
                        <i
                          class="el-icon-download"
                          style="padding-right: 2px"
                        ></i
                        >{{ $t("downloadPiPoint") }}</el-button
                      >
                    </el-tooltip>
                  </template>
                  <template v-else>
                    <el-tooltip
                      class="item"
                      effect="light"
                      :content="$t('downloadTemplateTip')"
                      placement="top-start"
                    >
                      <a href="/ElementTemplates.csv" download>
                        <i
                          class="el-icon-download"
                          style="padding-right: 2px"
                        ></i
                        >{{ $t("downloadTemplate") }}</a
                      >
                    </el-tooltip>
                    <el-tooltip
                      class="item"
                      effect="light"
                      :content="$t('downloadAfElementTip')"
                      placement="top-start"
                    >
                      <el-button
                        type="text"
                        style="padding-left: 16px"
                        @click="searchDatas($t('downloadAfElement'))"
                        :disabled="loading"
                      >
                        <i
                          class="el-icon-download"
                          style="padding-right: 2px"
                        ></i
                        >{{ $t("downloadAfElement") }}</el-button
                      >
                    </el-tooltip>
                  </template>
                  <el-tooltip
                    v-if="isEditable && p.value && p.value != '*'"
                    class="item"
                    effect="light"
                    :content="$t('downloadCSVInUseTip')"
                    placement="top-start"
                  >
                    <a
                      :href="downloadUrl + p.value"
                      download
                      style="padding-left: 16px"
                    >
                      <i class="el-icon-download" style="padding-right: 2px"></i
                      >{{ $t("downloadCSVInUse") }}</a
                    >
                  </el-tooltip>
                </div>
              </div>
              <div
                v-if="activeRadio == 'select_file'"
                class="description"
                v-html="transforHtml(p.description)"
              ></div>
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
                    p.hint.type == 'timeout' ||
                    p.hint?.type === 'duration'
                  "
                >
                  <el-input
                    size="small"
                    v-model="p.value"
                    :placeholder="p.placeholder"
                  ></el-input>
                </template>
                <template v-if="p.hint.type && p.hint.type === 'str'">
                  <div v-if="p.hint.choices" class="select-with-btn">
                    <el-select
                      v-if="
                        ['bucket', 'measurements', 'metrics'].includes(p.name)
                      "
                      v-model="p.value"
                      size="small"
                      placeholder=""
                      :style="
                        p.name === 'bucket'
                          ? {
                              width: '80%',
                              marginLeft: '-15px',
                              marginRight: '8px',
                            }
                          : { marginLeft: '-15px' }
                      "
                      :allow-create="true"
                      filterable
                      default-first-option
                      :multiple="p.multiple"
                      @change="(value) => changeBucket(value, p.name)"
                    >
                      <el-option
                        v-for="c in p.name == 'bucket'
                          ? bucketList
                          : p.name == 'metrics'
                          ? metricsList
                          : measurementList[0]?.children"
                        :key="c.id || c"
                        :label="c.id || c"
                        :value="c.id || c"
                      ></el-option>
                    </el-select>
                    <el-select
                      v-else
                      size="small"
                      v-model="p.value"
                      placeholder=""
                      style="margin-left: -15px"
                    >
                      <el-option
                        v-for="c in p.hint.choices"
                        :key="c"
                        :label="c"
                        :value="c"
                      ></el-option>
                    </el-select>
                    <template
                      v-if="tagName == 'opentsdb' && p.name == 'metrics'"
                    >
                      <el-button
                        style="margin-left: 10px"
                        v-if="p.name == 'metrics'"
                        size="small"
                        type="primary"
                        plain
                        :disable="btnLoading"
                        :loading="btnLoading"
                        @click="() => getMetrics(true)"
                        >{{ $t("datasource.getmetrics") }}</el-button
                      >
                    </template>
                    <el-button
                      v-else-if="p.name === 'bucket'"
                      size="small"
                      type="primary"
                      plain
                      :disable="btnLoading"
                      :loading="btnLoading"
                      @click="() => getSchema(true)"
                      >{{ $t("datasource.getschema") }}</el-button
                    >
                  </div>
                  <el-input v-else v-model="p.value" size="small"></el-input>
                </template>
                <template v-if="p.hint === 'bool' || p.hint.type === 'bool'">
                  <el-checkbox
                    size="small"
                    v-model="p.value"
                    true-label="true"
                    false-label="false"
                    :disabled="p.disabled"
                    @change="(value) => handelCheckbox(value, p.name)"
                  ></el-checkbox>
                </template>
                <template
                  v-if="
                    (p.hint.type && p.hint.type === 'integer') ||
                    p.hint === 'integer'
                  "
                >
                  <el-input-number
                    size="small"
                    v-model="p.value"
                    :min="p.hint.min"
                    :max="p.hint.max"
                  ></el-input-number>
                </template>
                <template v-if="p.hint == 'time' || p.hint?.type == 'time'">
                  <DatePicker
                    size="small"
                    v-model="p.value"
                    type="datetime"
                    v-if="p.name == 'beginTime' || p.name == 'endTime'"
                    :picker-options="
                      p.name == 'beginTime' ? startOption : endOption
                    "
                    :placeholder="p.placeholder"
                    @change="(value) => handleTime(value, p.name)"
                  >
                  </DatePicker>
                  <DatePicker
                    size="small"
                    v-model="p.value"
                    type="datetime"
                    v-if="p.name === 'start' || p.name == 'end'"
                    :picker-options="
                      p.name === 'start' ? startOption : endOption
                    "
                    :placeholder="p.placeholder"
                    @change="(value) => handleTime(value, p.name)"
                  >
                  </DatePicker>
                </template>
                <template v-if="p.hint && Array.isArray(p.hint)">
                  <el-radio-group v-model="p.value" class="radio-custom">
                    <el-radio
                      v-for="r in p.hint"
                      :key="r.display"
                      :label="r.type == 'constant' ? r.value : 'select_time'"
                      class="radio-flex"
                    >
                      <DatePicker
                        v-model="r.value"
                        value-format="yyyy-MM-dd HH:mm:ss"
                        type="datetime"
                        v-if="r.type == 'time'"
                        :picker-options="
                          p.name == 'BackfillStartTime'
                            ? backfillStartOption
                            : backfillEndOption
                        "
                        :placeholder="r.display"
                        size="small"
                      >
                      </DatePicker>
                      <span class="text-wrap" v-else>{{ r.display }}</span>
                    </el-radio>
                  </el-radio-group>
                </template>
                <div
                  v-if="p.name == 'BackfillStartTime'"
                  class="description"
                  v-html="transforHtml(p.short_description)"
                ></div>
                <div
                  v-else
                  class="description"
                  v-html="transforHtml(p.description)"
                ></div>
              </div>
            </div>
          </template>
        </section>
      </template>
      <section class="bottom">
        <el-button
          v-if="isShowEditBtn"
          class="edit-btn"
          type="primary"
          @click="edit"
          size="small"
          >{{ $t("edit") }}</el-button
        >
        <el-button
          v-else
          type="primary"
          @click="save"
          size="small"
          >{{ isEditable && !isCopyable ? $t("save") : $t("add") }}</el-button
        >
        <el-button @click="cancel" class="cancel-btn" size="small">{{
          $t("cancel")
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
    <DialogCreateDb></DialogCreateDb>
  </div>
</template>
<script>
import DataTarget from "./dataTarget.vue";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import {
  AddSource,
  EditSource,
  getUaAndDaData,
  downlaodAllNodes,
  validateTask,
} from "@/api/explorer/datain";
import DatePicker from "@/components/date-picker";
import { Message } from "element-ui";
import marked from "marked";
import { debounce, parsinginZone, decrypt } from "@/utils/index";
import DialogCreateDb from "../components/addDbDialog.vue";
import Result from "../components/result.vue";
export default {
  name: "DbSourceUI",
  components: { DatePicker, DialogCreateDb, DataTarget, Result },
  props: {
    dbsource: {
      type: Array,
      default() {
        return [];
      },
    },
    tagName: {
      type: String,
      default: "datasource",
    },
    isEditable: {
      type: Boolean,
      default: false,
    },
    editId: {
      type: Number,
      default: 0,
    },
    isCopyable: {
      type: Boolean,
    },
  },

  data() {
    const startTimeOption = (time) => {
      let endLsit = this.dbsource[0].groups.map((g) => {
        return g.params.filter(
          (item) => item.name == "endTime" || item.name == "end"
        );
      });
      let end = endLsit.filter((item) => item.length > 0)[0];

      if (end[0].value) {
        return time.getTime() > new Date(end[0].value).getTime();
      } else {
        return false;
      }
    };
    const endTimeOption = (time) => {
      let startLsit = this.dbsource[0].groups.map((g) => {
        return g.params.filter(
          (item) => item.name == "beginTime" || item.name == "start"
        );
      });
      let start = startLsit.filter((item) => item.length > 0)[0];

      if (start[0].value) {
        return (
          time.getTime() <
          new Date(start[0].value).getTime() - 24 * 60 * 60 * 1000
        );
      } else {
        return false;
      }
    };
    const backfillStart = (time) => {
      let end = this.dbsource[0].groups
        .filter((val) => val.name.includes("Backfill"))[0]
        .params.filter((item) => item.name == "BackfillEndTime");
      if (end[0]?.hint[0]?.value) {
        return time.getTime() > new Date(end[0]?.hint[0]?.value).getTime();
      } else {
        return false;
      }
    };
    const backfillEnd = (time) => {
      let start = this.dbsource[0].groups
        .filter((val) => val.name.includes("Backfill"))[0]
        .params.filter((item) => item.name == "BackfillStartTime");
      if (start[0]?.hint[0]?.value) {
        return (
          time.getTime() <
          new Date(start[0]?.hint[0]?.value).getTime() - 24 * 60 * 60 * 1000
        );
      } else {
        return false;
      }
    };
    return {
      language: localStorage.getItem("local_language"),
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
      activeName: "",
      textarea: "",
      isShowConfiguration: false,
      loading: false,
      configurationdata: [],
      activeDataSet: {},
      btnLoading: false,
      bucketList: [],
      measurementList: [],
      metricsList: [],
      piSystemConfiguration: "PI Data Archive and Asset Framework (AF) Server",
      isPiDataArchiveAll: true,
      limit: 1,
      uploadData: {
        req_id: new Date().getTime(),
      },
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      downloadUrl: process.env.VUE_APP_X_API + `/download?file_path=`,
      activeRadio: "select_file",
      isShowEditBtn: false,
      checkLoading: false,
      percentage: 0,
      checkResult: {
        // valid: false,
        // support: false,
        // data_source: "",
        // version: "", // 返回数据源版本，不能获得版本则不返回该字段。
      },
      activeCollapse: ''
    };
  },
  created() {
    this.getDatabases();
    if (this.isEditable) {
      this.handleEditData();
      let defaultVal =
        (this.dbsource[0]?.params && this.dbsource[0]?.params[0]?.value) ||
        this.piSystemConfiguration;
      this.changeSystemConfiguration(defaultVal);
      this.getSchema(false);
      this.isShowEditBtn = this.isCopyable ? false : true;
    } else {
      this.activeName = "point_file";
    }
  },
  mounted() {
    // this.activeName = this.dbsource[0].datasets
    //   ? this.dbsource[0].groups[0].params[0].name
    //   : "";
  },
  computed: {
    agentId() {
      return this.$store.state.app.currentAgentID || "";
    },
    sourceName() {
      return this.$store.state.app.currentDSName || "";
    },
    targetDatabase() {
      return this.$store.state.app.currentDBName || "";
    },
  },
  watch: {
    "$i18n.locale": {
      deep: true,
      handler(val) {
        this.language = val;
      },
    },
    dbsource: {
      deep: true,
      handler(val) {
        this.$forceUpdate();
      },
    },
    tagName: {
      deep: true,
      handler(val) {
        this.$forceUpdate();
      },
    },
    "$store.state.dbs.dialogDbVisible": {
      handler(val) {
        if (!val) {
          this.getDatabases();
        }
      },
    },
    "$store.state.app.currentDBType": {
      handler(val) {
        if (!this.isEditable && val == "pibackfill") {
          this.handelAddData();
        }
      },
    },
  },
  methods: {
    handleEditData() {
      this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
        group.params.map((p) => {
          if ((p.hint === "time" || p.hint?.type === "time") && p.value) {
            // 时间返回值适配时区后再根据 placeholder字段 格式化
            if (
              p.name == "start" ||
              p.name == "end" ||
              p.name == "beginTime" ||
              p.name == "endTime"
            ) {
              p.value = parsinginZone(p.value);
            } else {
              p.value = parsinginZone(p.value, p.placeholder);
            }
          }
          if (p.multiple && p.value && typeof p.value == "string") {
            // 多选下拉框的返回值改为数组
            let newVal = p.value.split();
            p.value = newVal;
          }
          if (p.name == "BackfillStartTime" || p.name == "BackfillEndTime") {
            if (p.value && p.value !== "auto") {
              p.hint[0].value = p.value;
              p.value = "select_time";
            } else {
              p.value = "select_time";
            }
          }
          return p;
        });
        return group;
      });
      if (this.dbsource[0]?.datasets) {
        this.dbsource[0].datasets.params =
          this.dbsource[0]?.datasets?.params.map((p) => {
            if (p.value) {
              if (p.value != "*") {
                p.fileList = [].concat({
                  name: p.value?.substr(p.value.lastIndexOf("/") + 1),
                  percentage: 100,
                  raw: File,
                  response: [].concat(p.value),
                  size: 87,
                  status: "success",
                  uid: 1,
                });
              }
              this.activeRadio = p.value.includes("@")
                ? "select_file"
                : "all_points";
              p.value = p.value?.substr(p.value.lastIndexOf("@") + 1);
              this.activeName = p.name;
            } else {
              this.activeName = "point_file";
            }
            return p;
          });
      }
    },
    handelAddData() {
      this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
        group.params.map((p) => {
          if (p.name == "BackfillStartTime" || p.name == "BackfillEndTime") {
            p.value = "select_time";
          }
          return p;
        });
        return group;
      });
    },
    handleSuccess(response, file, fileList, name) {
      this.dbsource[0].datasets.params = this.dbsource[0].datasets.params.map(
        (p) => {
          if (p.name == name) {
            p.fileList =
              fileList?.length <= 1 ? fileList : [{ ...fileList[1] }];
            p.value = file.response[0];
          }
          return p;
        }
      );
    },
    handleRemove(name) {
      this.dbsource[0].datasets.params = this.dbsource[0].datasets.params.map(
        (p) => {
          if (p.name == name) {
            p.fileList = [];
            p.value = "";
          }
          return p;
        }
      );
    },
    handlePreview(file_path) {
      let link = document.createElement("a");
      link.download = "file_name";
      link.href = this.downloadUrl + file_path;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    },
    handelCheckbox(value, name) {
      this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
        if (group.name == "Backfill" || group.name == "历史填充（Backfill）") {
          group.params.map((p) => {
            if (
              name == "FromTDengineLastTime" &&
              p.name == "ToTDengineFirstTime"
            ) {
              p.disabled = value == "true" ? true : false;
            } else if (
              name == "ToTDengineFirstTime" &&
              p.name == "FromTDengineLastTime"
            ) {
              p.disabled = value == "true" ? true : false;
            }
            return p;
          });
        }
        return group;
      });
    },
    handleTime(time, name) {
      if (time) {
        this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
          group.params.map((p) => {
            if (p.name == name) {
              // RFC3339 时间格式，带时区
              p.value = parsinginZone(time);
            }
            return p;
          });
          return group;
        });
      }
    },
    changeSystemConfiguration(val) {
      this.piSystemConfiguration = val;
      this.isPiDataArchiveAll =
        val == "PI Data Archive and Asset Framework (AF) Server";
      if (val == "PI Data Archive Only") {
        this.activeName = "point_file";
      }
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
        !Object.is(val, "undefined") &&
        !Object.is(val, "null")
      );
    },
    edit() {
      this.isShowEditBtn = false;
    },

    save() {
      if (this.isEditable && !this.isCopyable) {
        this.$confirm(this.$t("dataIn.saveTip"), this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.submit(true);
          })
          .catch(() => {});
      } else {
        this.submit(true);
      }
    },

    clickCheckBtn() {
      this.checkResult = this.$options.data().checkResult;
      this.submit(false);
    },
    // 数据源可用性和版本检查
    async getValidateResult(dns) {
      try {
        this.checkLoading = true;
        let result = await validateTask(dns, this.agentId);
        console.log("result", result);
        this.checkResult = result;
        this.checkLoading = false; // 检测的 loading 效果
        this.activeCollapse = 'one'
      } catch (error) {
        this.checkLoading = false;
        console.log("err");
      }
    },

    async submit(isSubmit) {
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
              data.options[key]["value"] == undefined) &&
            this.isPiDataArchiveAll
          ) {
            Message({
              type: "warning",
              message: `${enterTip} ${data.options[key].display} `,
            });
            return;
          }
        }
        if (!this.sourceName && isSubmit) {
          Message.warning(`${enterTip} ${this.$t("name")}`);
          return;
        }
        if (!this.targetDatabase && isSubmit) {
          Message.warning(`${enterTip} ${this.$t("stream.targetDB")}`);
          return;
        }
        if (this.tagName === "taos") {
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
        } else if (this.tagName == "datasource") {
          // data.options.endpoint.value.replace(/(taos\+|tmq\+)/g, "");
          // if (data.options.endpoint.value.includes("://")) {
          //   dns =
          //     "+" + data.options.endpoint.value.replace(/(taos\+|tmq\+)/g, "");
          // } else {
          //   dns =
          //     "://" +
          //     data.options.endpoint.value.replace(/(taos\+|tmq\+)/g, "");
          // }
          let url = data.options.endpoint.value.replace(/(taos\+|tmq\+)/g, "");
          if (url.includes('://')) {
            let parsed_url = new URL(url);
            let scheme = null;
            if (parsed_url.protocol == 'http:') {
              scheme = '+ws'
            } else if (parsed_url.protocol == 'https:') {
              scheme = '+wss'
            } else {
              scheme = '+' + parsed_url.protocol.replace(':', '')
            }

            let host = parsed_url.host;
            let user =  parsed_url.username || localStorage.getItem('username') || '';
            let decrypted = encodeURI(decrypt(localStorage.getItem('pwd')));
            let pass = parsed_url.password || decrypted || '';
            dns = scheme + '://' + user + ':' + pass + '@' + host + parsed_url.pathname + parsed_url.search;
          } else {
            let host = url;
            let user = localStorage.getItem('username') || '';
            let decrypted = encodeURI(decrypt(localStorage.getItem('pwd')));
            let pass = decrypted || '';
            dns = '+ws://' + host;
          }
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

        dns +=
          data.options.subject?.value && this.isPiDataArchiveAll
            ? "/" + data.options.subject?.value
            : "";
        let reg = /\s+/g;
        dns = dns.replace(reg, "").trim();
        let querystr = "";

        if (data.groups && isSubmit) {
          for (let index = 0; index < data.groups.length; index++) {
            //   for (let j = 0; j < data.groups[index].params.length; j++) {
            for (let g of Object.keys(data.groups[index].params)) {
              if (
                Object.hasOwnProperty.call(
                  data.groups[index].params[g],
                  "required"
                ) &&
                !this.handleEmptyValue(data.groups[index].params[g]["value"])
              ) {
                Message({
                  type: "warning",
                  message: `${enterTip} ${data.groups[index].params[g].name} `,
                });
                return;
              } else {
                if (this.handleEmptyValue(data.groups[index].params[g].value)) {
                  if (
                    data.groups[index].params[g].hint &&
                    Array.isArray(data.groups[index].params[g].hint)
                  ) {
                    if (data.groups[index].params[g].value == "auto") {
                      querystr +=
                        `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                        "&";
                    } else {
                      // debugger
                      if (
                        this.handleEmptyValue(
                          data.groups[index].params[g].hint[0].value
                        )
                      ) {
                        querystr +=
                          `${data.groups[index].params[g].name}=${data.groups[index].params[g].hint[0].value}` +
                          "&";
                      }
                    }
                  } else {
                    querystr +=
                      `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                      "&";
                  }
                }
              }
            }
            //   }
          }
        }

        // datasets.categories is not used since 9adc5721
        if (data.datasets && data.datasets.categories && isSubmit) {
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
        if (data.datasets && data.datasets.params && isSubmit) {
          for (let index = 0; index < data.datasets.params.length; index++) {
            if (data.datasets.params[index].name == this.activeName) {
              if (this.activeRadio == "select_file") {
                if (
                  this.handleEmptyValue(data.datasets.params[index].value) &&
                  data.datasets.params[index].value != "*"
                ) {
                  querystr +=
                    `${data.datasets.params[index].name}=@${data.datasets.params[index].value}` +
                    "&";
                } else {
                  Message({
                    type: "warning",
                    message: this.$t("datasource.uploadtip"),
                  });
                  return;
                }
              } else {
                querystr += `${data.datasets.params[index].name}=*` + "&";
              }
            }
          }
        }
        if (data.params) {
          if (this.isPiDataArchiveAll) {
            for (let index = 0; index < data.params.length; index++) {
              if (
                Object.hasOwnProperty.call(data.params[index], "required") &&
                data.params[index]["value"] == ""
              ) {
                Message({
                  type: "warning",
                  message: `${enterTip} ${data.params[index].name} `,
                });
                return;
              } else {
                if (this.handleEmptyValue(data.params[index].value)) {
                  querystr +=
                    `${data.params[index].name}=${data.params[index].value}` +
                    "&";
                }
              }
            }
          } else {
            querystr += `${data.params[0].name}=${data.params[0].value}&`;
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
        }
        dns += querystr
          ? (dns.includes("?") ? "&" : "?") + querystr.replace(/&$/g, "")
          : "";

        let apiParams = {
          from:
            (this.tagName === "datasource" ? "tmq" : "taos") +
            (data.protocol
              ? Object.is(data.protocol.value, "--") || !data.protocol.value
                ? ""
                : "+"
              : "") +
            dns,
          name: this.sourceName,
          to:
            "taos+" +
            localStorage.getItem("base_url") +
            (this.targetDatabase ? "/" + this.targetDatabase : ""),
          labels: [
            "type::datain",
            `cluster-id::${id}`,
            `user::${localStorage.getItem("username")}`,
          ],
        };
        if (this.agentId) {
          apiParams["via"] = this.agentId;
        }
        if (this.tagName === "datasource" || this.tagName === "taos") {
          if (isSubmit) {
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
            this.getValidateResult(apiParams.from);
          }
        } else {
          let piParams = {
            from:
              this.tagName == "influxdb"
                ? "influxdb" +
                  (data.protocol
                    ? Object.is(data.protocol.value, "--")
                      ? ""
                      : "+"
                    : "") +
                  dns
                : this.tagName == "opentsdb"
                ? this.tagName + (data.protocol?.value ? "+" : "") + dns
                : this.tagName + dns,
            name: this.sourceName,
            //   + (data.protocol?(Object.is(data.protocol.value, "--") ? "" : "+"):'') + dns,
            // name: localStorage.getItem("datainName"),
            to:
              "taos+" +
              localStorage.getItem("base_url") +
              (this.targetDatabase ? "/" + this.targetDatabase : ""),
            labels: [
              "type::datain",
              `cluster-id::${id}`,
              `user::${localStorage.getItem("username")}`,
            ],
          };
          if (this.agentId) {
            piParams["via"] = this.agentId;
          }
          console.log(this.isEditable, this.editId, "编辑");
          if (isSubmit) {
            if (this.isEditable && this.editId&& !this.isCopyable) {
              let result = await EditSource(piParams, this.editId);
              if (result.message) {
                Message.error(result.message);
                return;
              }
              this.$parent.changeEditable(false);
              this.$parent.toggleComponent("pitable", "");
            } else {
              let result = await AddSource(piParams);
              if (result.message) {
                Message.error(result.message);
                return;
              }
              if (result && result.id) {
                this.$parent.changeEditable(false);
                this.$parent.toggleComponent("pitable");
                Message.success("Operation Successfully!");
              }
            }
            // if (this.isEditable && this.editId && !this.isCopyable) {
            //   let result = await EditSource(piParams, this.editId);
            //   if (result.message) {
            //     Message.error(result.message);
            //     return;
            //   }
            // }
          } else {
            console.log("ss", piParams);
            this.getValidateResult(piParams.from);
          }
        }
      } catch (err) {
        console.error(err);
        (err.response &&
          err.response.data &&
          err.response.data.message &&
          Message.error(err.response.data.message)) ||
          Message.error(err);
      }
    },

    cancel() {
      this.$parent.currentName = "dbsource";
    },

    handleDbBtn() {
      this.$store.commit("dbs/HANDLE_ADD_DB");
      this.$store.commit("dbs/SET_ADD_DB_COMP", "datain");
      this.$store.commit("dbs/SET_DIALOG_DB_VISABLE", true);
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
    // handelDataSet(data) {
    //   this.activeDataSet = data;
    //   let categories = [];
    //   if (!Object.hasOwnProperty.call(data, "options")) {
    //     categories = this.dbsource[0].datasets.categories.map((cate) => {
    //       if (cate.category == data.category) {
    //         if (Array.isArray(cate.target.value)) {
    //           cate.target.value.push(data.id);
    //           cate.target.value = Array.from(new Set(cate.target.value));
    //         } else {
    //           cate.target.value = data.id;
    //         }
    //       }
    //       return cate;
    //     });
    //     this.dbsource[0].datasets.categories = categories;
    //   }
    // },
    downloadAllPoints(data, name) {
      downlaodAllNodes(data)
        .then((res) => {
          let blob = new Blob([res], { type: "text/csv,charset=UTF-8" });
          let link = document.createElement("a");
          link.download = `${name}.csv`;
          link.style.display = "none";
          link.href = URL.createObjectURL(blob);
          document.body.appendChild(link);
          link.click();
          URL.revokeObjectURL(link.href);
          document.body.removeChild(link);
          this.loading = false;
        })
        .catch((err) => {
          this.loading = false;
        });
    },
    searchDatas: debounce(function (name) {
      try {
        let data = this.dbsource[0];
        let host = data.options.host.value ? data.options.host.value : "";
        let subject =
          data.options.subject.value && this.isPiDataArchiveAll
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
        if (!subject && this.isPiDataArchiveAll) {
          Message({
            type: "warning",
            message: `${enterTip} ${data.options.subject.display}`,
          });
          return;
        }
        let querystr = "";
        if (data.params) {
          if (this.isPiDataArchiveAll) {
            for (let index = 0; index < data.params.length; index++) {
              if (
                Object.hasOwnProperty.call(data.params[index], "required") &&
                data.params[index]["value"] == ""
              ) {
                Message({
                  type: "warning",
                  message: `${enterTip} ${data.params[index].name} `,
                });
                return;
              } else {
                if (this.handleEmptyValue(data.params[index].value)) {
                  querystr +=
                    `${data.params[index].name}=${data.params[index].value}` +
                    "&";
                }
              }
            }
          } else {
            querystr += `${data.params[0].name}=${data.params[0].value}&`;
          }
        }
        let params = null;
        // params = {
        //   from: `${this.tagName}://${host}${subject}${
        //     querystr ? "?" + querystr.replace(/&$/g, "") : ""
        //   }`,
        //   categories: [this.activeName],
        //   pattern: e.target.value,
        //   pattern: '.*',
        //   offset: 0,
        //   limit: 10,
        // };
        // const viaObj = {
        //   via: this.agentId,
        // };
        // if (viaObj.via) {
        //   Object.assign(params, viaObj);
        // }
        let categories = "";
        switch (this.activeName) {
          case "point_file":
            categories = "PointList";
            break;
          case "template_for_pi_point_file":
            categories = "TemplateForPIPoint";
            break;
          case "template_for_af_element_file":
            categories = "TemplateForAFElement";
            break;
          default:
            break;
        }
        let from = `${this.tagName}://${host}${subject}${
          querystr ? "?" + querystr.replace(/&$/g, "") : ""
        }
            &categories=${categories}`;
        if (this.agentId) {
          from += `&via=${this.agentId}`;
        }
        this.loading = true;
        // getUaAndDaData(params)
        //   .then((res) => {
        //     if (res && res.code && res.code != 0) {
        //       Message({
        //         type: "error",
        //         message: res && res.message,
        //       });
        //     } else {
        //       this.configurationdata = res;
        //       Message({
        //         type: "success",
        //         message: this.$t("operateSucc"),
        //       });
        //     }
        //     this.loading = false;
        //   })
        //   .catch((err) => {
        //     Message({
        //       type: "error",
        //       message: err,
        //     });
        //   });
        this.downloadAllPoints(from, name);
      } catch (error) {
        this.loading = false;
      }
    }, 100),
    changeBucket(value, name, choices) {
      if (name === "bucket") {
        this.measurementList = this.bucketList.filter(
          (item) => item.id == value
        );
        // 清空 Measurements
        this.dbsource[0].groups = this.dbsource[0].groups.map((group) => {
          group.params.map((p) => {
            if (p.name === "measurements") {
              p.value = [];
            }
            return p;
          });
          return group;
        });
      }
    },
    //opentsdb获取metrics
    async getMetrics() {
      this.btnLoading = true;
      let data = this.dbsource[0];
      let obj = {
        from:
          "opentsdb+" +
          data.protocol.value +
          "://" +
          data.options.host.value +
          ":" +
          data.options.port.value,
        name: "",
        categories: ["nodes"],
        pattern: "api",
        offset: 0,
        limit: 10,
      };
      let result = await getUaAndDaData(obj);
      this.btnLoading = false;
      if (result && result.message) {
        Message.error(result.message);
        return;
      }
      this.metricsList = JSON.parse(result[0].id);
    },
    getSchema(isNeedTip) {
      this.btnLoading = true;
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
              data.options[key]["value"] == undefined) &&
            this.isPiDataArchiveAll
          ) {
            Message({
              type: "warning",
              message: `${enterTip} ${data.options[key].display} `,
            });
            this.btnLoading = false;
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
              this.btnLoading = false;
              return;
            }
          }
          dns += `://${data.options.host.value ? data.options.host.value : ""}`;
        }

        if (data.options.port) {
          if (!this.isPort && this.tagName == "influxdb") {
            this.btnLoading = false;
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

        dns +=
          data.options.subject &&
          data.options.subject.value &&
          this.isPiDataArchiveAll
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
            if (p.value && p.value != undefined) {
              querystr += `${p.name}=${p.value}&`;
            }
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
        dns += querystr
          ? (dns.includes("?") ? "&" : "?") + querystr.replace(/&$/g, "")
          : "";

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
          categories: ["nodes"],
          pattern: "api",
          offset: 0,
          limit: 10,
        };
        if (this.agentId) {
          apiParams["via"] = this.agentId;
        }
        if (this.tagName === "datasource") {
          getUaAndDaData(apiParams)
            .then((res) => {
              console.log("apiParams", res);
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
                ? "influxdb" +
                  (data.protocol
                    ? Object.is(data.protocol.value, "--")
                      ? ""
                      : "+"
                    : "") +
                  dns
                : this.tagName + dns,
            categories: ["nodes"],
            pattern: "api",
            offset: 0,
            limit: 10,
          };
          if (this.agentId) {
            piParams["via"] = this.agentId;
          }
          getUaAndDaData(piParams)
            .then((res) => {
              if (res && res.code && res.code != 0) {
                Message({
                  type: "error",
                  message: res && res.message,
                });
              } else {
                this.bucketList =
                  res[0].id !== "" &&
                  Object.keys(JSON.parse(res[0].id)).map((item) => {
                    return { id: item, children: JSON.parse(res[0].id)[item] };
                  });
                if (this.isEditable) {
                  let bucketVal = this.dbsource[0].groups[0].params[0].value;
                  this.changeBucket(bucketVal, "bucket");
                }
                if (isNeedTip) {
                  Message({
                    type: "success",
                    message: this.$t("operateSucc"),
                  });
                }
              }
              this.btnLoading = false;
            })
            .catch((err) => {
              this.btnLoading = false;
              if (isNeedTip) {
                Message({
                  type: "error",
                  message: err,
                });
              }
            });
        }
      } catch (err) {
        this.btnLoading = false;
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.source-ui {
  // padding-left: 20px;
  justify-content: space-between;
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
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 100;
    }
  }

  .left-ui {
    width: 50%;
    flex-shrink: 0;
    .description {
      max-width: 568px;
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
    section {
      border: 1px solid #ececef;
      margin-bottom: 20px;
      border-radius: 12px;
      padding: 15px;
      // border-bottom: 1px solid #ececef;
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
      font-weight: 500;
      align-items: center;
      width: 200px;
      display: block;
      white-space: normal;
      flex-shrink: 0;
    }
    .no-label {
      align-items: center;
      width: 8px;
    }
    .label.required,
    .no-label.required {
      position: relative;
      &::after {
        content: "*";
        // position: absolute;
        color: red;
        font-size: 14px;
        line-height: 25px;
        right: 0px;
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
      .pi-tab-item {
        display: block;
        margin-bottom: 0;
        ::v-deep .el-tab-pane {
          display: flex;
          flex-wrap: wrap;
          flex-direction: column;
        }
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
      .radio-flex {
        display: flex;
        align-items: center;
      }
    }

    .upload-dataset {
      display: flex;
      white-space: nowrap;
      align-items: baseline;
      // margin-bottom: 8px;
    }
    .upload-flex {
      display: flex;
      white-space: nowrap;
      align-items: baseline;
      margin-bottom: 8px;
    }
    ::v-deep .el-upload-list__item .el-icon-close-tip {
      display: none !important;
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
    :deep {
      .el-input-number__increase,
      .el-input-number__decrease {
        height: 30px;
        display: flex;
        justify-content: center;
        align-items: center;
      }
    }
  }
  .right-ui {
    flex: 1;
    margin-left: 40px;
    :deep {
      .v-note-panel {
        border-radius: 12px;
      }
    }
  }
  .description {
    display: initial !important;
    color: #acaab2;
    margin-bottom: 8px !important;
    white-space: normal !important;
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
    .resultWrap {
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
            white-space: normal;
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
  .cancel-btn,
  .edit-btn,
  .upload-flex .item {
    z-index: 101;
  }

  ::v-deep .el-tabs__item {
    max-width: 230px;
    line-height: 22px !important;
    display: table-cell;
    vertical-align: middle;
    white-space: pre-wrap;
    word-wrap: break-word;
  }
  .text-wrap {
    white-space: pre-wrap;
    word-wrap: break-word;
  }

  ::v-deep .radio-custom {
    display: flex;
    flex-direction: column;
  }
  ::v-deep .el-upload-list {
    max-width: 90px;
  }
}
</style>
