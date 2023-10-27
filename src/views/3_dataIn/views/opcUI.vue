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

      
      <section class="basics" v-if="tagName !== 'csv'">
        <div class="block-title">
          <span>{{ $t("dataIn.connectionConfiguration") }}</span>
        </div>
        <div class="protocol" v-if="dbsource[0].protocol">
          <span class="label">{{ dbsource[0].protocol.display }}</span>
          <div class="label-value">
            <el-select
              size="small"
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
                size="small"
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
                          size="small"
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
                          size="small"
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
                <template v-else>
                  <div
                    v-for="(p, index) in at.params"
                    :key="index"
                    :style="
                      textareas.includes(p.name) ? styleareaobj : styleobj
                    "
                  >
                    <span
                      :class="['label', p.required ? 'required' : '']"
                      :style="
                        textareas.includes(p.name)
                          ? { 'padding-top': '10px!important' }
                          : {}
                      "
                    >
                      <el-tooltip
                        class="item"
                        effect="light"
                        placement="top"
                        v-if="
                          ['security_mode', 'security_policy'].includes(p.name)
                        "
                      >
                        <div
                          v-html="transforHtml(p.description)"
                          slot="content"
                        ></div>
                        <i class="el-icon-info"></i>
                      </el-tooltip>

                      {{ p.display }}
                    </span>

                    <div style="flex: 1">
                      <template v-if="p.hint && p.hint.choices">
                        <el-select
                          size="small"
                          v-model="p.value"
                          placeholder=""
                          style="
                            margin-left: 0px;
                            width: 100%;
                            margin-bottom: 8px;
                          "
                          :disabled="
                            p.name === 'security_policy' && policyDisabled
                          "
                          @change="handleAuthentication(p)"
                        >
                          <el-option
                            v-for="c in p.hint.choices"
                            :key="c"
                            :label="c"
                            :value="c"
                          ></el-option>
                        </el-select>
                      </template>
                      <template v-if="p.hint && p.hint.type == 'file'">
                        <el-upload
                          class="upload-demo"
                          ref="upload"
                          :data="uploadData"
                          :action="uploadUrl"
                          :on-success="
                            p.name == 'certificate'
                              ? handleCertSuccess
                              : handlePrivateSuccess
                          "
                          :file-list="
                            p.name == 'certificate'
                              ? certfileList
                              : privatefileList
                          "
                          :auto-upload="true"
                        >
                          <el-button
                            slot="trigger"
                            size="small"
                            type="primary"
                            >{{ $t("datasource.selectfile") }}</el-button
                          >
                        </el-upload>
                      </template>
                      <el-input
                        size="small"
                        v-if="
                          p.hint && !p.hint.choices && p.hint.type !== 'file'
                        "
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
                        v-if="
                          !['security_mode', 'security_policy'].includes(p.name)
                        "
                        v-html="transforHtml(p.description)"
                      ></div>
                    </div>
                  </div>
                </template>
              </el-tab-pane>
            </template>
          </el-tabs>
        </div>
      </section>
      <section v-if="tagName !=='csv'">
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
      <section :class="['groups-dataset']" v-if="dbsource[0]?.datasets">
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
          <el-tabs
            v-model="dbsource[0].datasets.value"
            @tab-click="handleClick"
          >
            <el-tab-pane
              v-for="(p, pind) in dbsource[0].datasets.categories"
              :label="p.display"
              :name="p.category"
              :key="p.category"
              lazy
            >
              <div
                style="
                  margin-bottom: 10px;
                  display: flex;
                  align-items: baseline;
                "
              >
                <template v-if="p.category == 'csv_config_file'">
                  <el-upload
                    class="upload-demo"
                    ref="upload"
                    accept=".csv"
                    :on-remove="handleopcRemove"
                    :data="uploadData"
                    :action="uploadUrl"
                    :on-success="handleopcSuccess"
                    :file-list="opcfileList"
                    :auto-upload="true"
                  >
                    <el-button slot="trigger" size="small" type="primary">{{
                      $t("datasource.selectfile")
                    }}</el-button>
                  </el-upload>
                  <div class="download_typefiles">
                    <el-tooltip
                      placement="top"
                      :content="$t('dataIn.downloadtpltip')"
                      effect="light"
                    >
                      <template v-if="language.includes('zh')">
                        <a
                          href="/template-zh.csv"
                          download
                          style="margin-left: 15px"
                          ><i class="el-icon-download"></i
                          >{{ $t("dataIn.downloadtpl") }}</a
                        >
                      </template>
                      <template v-else>
                        <a
                          href="/template-en.csv"
                          download
                          style="margin-left: 15px"
                          ><i class="el-icon-download"></i
                          >{{ $t("dataIn.downloadtpl") }}</a
                        >
                      </template>
                    </el-tooltip>
                    <el-tooltip
                      placement="top"
                      :content="$t('dataIn.downloadnodestip')"
                      effect="light"
                    >
                      <span
                        v-loading="allnodesloading"
                        :class="[
                          'allnodes',
                          disableallnodeclick ? 'click' : 'noclick',
                        ]"
                        @click="downloadopcAllponits"
                      >
                        <i class="el-icon-download"> </i>
                        {{ $t("dataIn.downloadnodes") }}
                      </span>
                    </el-tooltip>
                    <el-tooltip
                      v-if="isEditable && opcinusefile"
                      placement="top"
                      :content="$t('dataIn.csvinusetip')"
                      effect="light"
                    >
                      <a
                        :href="downloadUrl + opcinusefile"
                        download
                        style="padding-left: 16px"
                      >
                        <i
                          class="el-icon-download"
                          style="padding-right: 2px"
                        ></i
                        >{{ $t("downloadCSVInUse") }}</a
                      >
                    </el-tooltip>
                  </div>
                </template>
                <ul v-else style="flex: 1">
                  <li
                    v-for="(all, ain) in p.params"
                    :key="ain"
                    style="display: flex; margin-bottom: 20px"
                  >
                    <span :class="['label', all.required ? 'required' : '']">{{
                      all?.display
                    }}</span>
                    <div style="flex: 1">
                      <template v-if="all?.hint.choices">
                        <el-select
                          v-model="all.value"
                          size="small"
                          style="width: 100%"
                        >
                          <el-option
                            v-for="item in all.hint.choices"
                            :key="item"
                            :value="item"
                            :label="item"
                          ></el-option>
                        </el-select>
                      </template>
                      <template v-else>
                        <el-input
                          size="small"
                          v-model="all.value"
                          style="margin-bottom: 8px"
                        ></el-input>
                      </template>
                      <div
                        class="description"
                        v-html="transforHtml(all.description)"
                      ></div>
                    </div>
                  </li>
                </ul>
              </div>

              <div :key="pind">
                <div
                  class="description"
                  v-html="transforHtml(p.description)"
                ></div>
                <div class="target">
                  <!-- <span
                    :class="['no-label', p.target.required ? 'required' : '']"
                  ></span> -->
                  <!-- <template v-if="p.target.multiple">
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
                  </template> -->
                  <!-- <el-button
                    size="medium"
                    @click="handleSelBtn"
                    style="height: 42px"
                    >{{ $t("datasource.select") }}</el-button
                  > -->
                </div>
                <div class="configuration" v-if="isShowConfiguration">
                  <el-input
                    size="small"
                    :placeholder="$t('datasource.regexPlaceholder')"
                    v-model="p.value"
                    :disable="p.target.selectable"
                    @keydown.enter.native="searchDatas($event, p.value)"
                  >
                    <el-button
                      size="small"
                      slot="append"
                      icon="el-icon-search"
                      @click="searchDatas($event, p.value)"
                    ></el-button>
                  </el-input>
                  <div class="resultWrap">
                    <div class="searchList" v-loading="loading">
                      <el-empty
                        :image-size="80"
                        v-if="configurationdata.length <= 0"
                      ></el-empty>
                      <template v-else>
                        <el-table
                          :data="configurationdata"
                          size="mini"
                          @row-click="handelDataSet"
                          highlight-current-row
                        >
                          <el-table-column
                            prop="id"
                            label="Id"
                          ></el-table-column>
                          <el-table-column
                            prop="name"
                            label="Name"
                          ></el-table-column>
                        </el-table>
                      </template>
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
                              {{ o.display }}
                            </span>
                            <el-input
                              placeholder=""
                              v-model="o.value"
                              size="small"
                            />
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
      <template v-for="(item, gind) in dbsource[0].groups">
        <section
          :class="[
            'groups',
            ['库表配置', 'Table Config'].includes(item.name)
              ? 'tableconfig'
              : item.name,
          ]"
          :key="gind"
        >
          <!-- opcPointavalible ? 'avalible' : 'notallowed', -->
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
              <span style="color: #4259ce; margin-right: 10px">SSL/TLS</span>
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
                      (p.hint && p.hint.type == 'file')
                    "
                  >
                    <template v-if="p.hint && p.hint.type == 'file'">
                      <el-upload
                        class="upload-demo"
                        ref="upload"
                        :data="uploadData"
                        :action="uploadUrl"
                        :on-success="
                          p.name == 'ca'
                            ? handleMqttCaSuccess
                            : p.name == 'cert'
                            ? handleMqttCertSuccess
                            : handleMqttCertKeySuccess
                        "
                        :file-list="
                          p.name == 'ca'
                            ? mqttcafile
                            : p.name == 'cert'
                            ? mqttcertfile
                            : mqttcertkeyfile
                        "
                        :auto-upload="true"
                      >
                        <el-button slot="trigger" size="small" type="primary">{{
                          $t("datasource.selectfile")
                        }}</el-button>
                      </el-upload>
                    </template>
                    <template v-else>
                      <el-input
                        size="small"
                        v-model="p.value"
                        :placeholder="p.placeholder ? p.placeholder : ''"
                        :type="text"
                      ></el-input>
                    </template>
                  </template>
                  <template v-if="p.hint && p.hint.type === 'str'">
                    <template v-if="p.hint.choices">
                      <el-select v-model="p.value" placeholder="" size="small">
                        <el-option
                          v-for="c in p.hint.choices"
                          :key="c"
                          :label="c"
                          :value="c"
                        ></el-option>
                      </el-select>
                    </template>
                    <el-input
                      size="small"
                      v-else
                      v-model="p.value"
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input>
                  </template>
                  <template
                    v-if="
                      (p.hint === 'bool' ||
                        (p.hint && p.hint.type === 'bool')) &&
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
                  <template v-else-if="p.hint && p.hint.type === 'bool'">
                    <!-- <p-three-checkbox :data="checkboxData" v-model="p.value" /> -->

                    <el-switch
                      v-model="p.value"
                      :active-value="'true'"
                      :inactive-value="'false'"
                    ></el-switch>
                  </template>
                  <template
                    v-if="
                      (p.hint && p.hint.type === 'integer') ||
                      p.hint === 'integer'
                    "
                  >
                    <el-input-number
                      size="small"
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
            <template v-for="p in item.params">
              <div :key="p.name" v-if="!item.name.includes('SSL')">
                <span :class="['label', p.required ? 'required' : '']">
                  {{ p.display ? p.display : p.name }}
                </span>
                <div class="label-value">
                  <template v-if="p.hint && p.hint.type == 'file'">
                    <el-upload
                      class="upload-demo"
                      ref="upload"
                      accept=".csv"
                      :limit="limit"
                      :data="uploadData"
                      :action="uploadUrl"
                      :on-success="handleSuccess"
                      :file-list="fileList"
                      :auto-upload="true"
                    >
                      <el-button
                        slot="trigger"
                        size="small"
                        type="primary"
                        style="margin-right: 20px"
                        >{{ $t("datasource.selectfile") }}</el-button
                      >
                     
                      <template v-if="language.includes('zh')">
                        <a href="/template-zh.csv" download>下载模板</a>
                      </template>
                      <template v-else>
                        <a href="/template-en.csv" download>Template</a>
                      </template>
                    </el-upload>
                  </template>
                  <template v-if="p.hint === 'str' || p.hint === 'timeout'">
                    <el-input
                      size="small"
                      v-model="p.value"
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input>
                  </template>
                  <template v-if="p.hint?.type && p.hint?.type === 'str'">
                    <template v-if="p.hint.choices">
                      <el-select
                        size="small"
                        v-model="p.value"
                        :placeholder="p.placeholder ? p.placeholder : ''"
                        @change="changeOpcCollectMode"
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
                      size="small"
                      v-else
                      v-model="p.value"
                      :placeholder="p.placeholder ? p.placeholder : ''"
                    ></el-input>
                  </template>
                  <template
                    v-if="
                      (p.hint === 'bool' || p.hint?.type === 'bool') &&
                      p.name == 'clean_session'
                    "
                  >
                    <el-radio-group v-model="p.value" v-if="p.choices">
                      <el-radio v-for="c in p.choices" :key="c" :label="c">
                        {{ c }}
                      </el-radio>
                    </el-radio-group>
                    <template v-else>
                      <!-- <el-checkbox
                        v-model="p.value"
                        true-label="true"
                        false-label="false"
                      ></el-checkbox> -->

                      <el-switch
                        v-model="p.value"
                        :active-value="'true'"
                        :inactive-value="'false'"
                      ></el-switch>
                    </template>
                  </template>
                  <template v-else-if="p.hint?.type && p.hint?.type === 'bool'">
                    <!-- <p-three-checkbox
                      :data="checkboxData"
                      v-model="p.value"
                      @changeThreeCheckbox="getThreeBoxNum($event, p)"
                    /> -->
                    <!-- <span style="color:purple;font-size:36px;">{{ p.name }} </span> -->
                    <el-switch
                      v-model="p.value"
                      :active-value="'true'"
                      :inactive-value="'false'"
                    ></el-switch>
                  </template>
                  <template
                    v-if="
                      (p.hint && p.hint.type === 'integer') ||
                      p.hint === 'integer'
                    "
                  >
                    <el-input-number
                      size="small"
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
              <!-- <template v-if="p.name == 'opc_table_config'">
                <div
                  :key="pind"
                  :class="[
                    'opcconf',
                    opcPointavalible ? 'avalible' : 'notallowed',
                  ]"
                >
                  <opcConnector
                    :opcConfig="opcConfig"
                    :isEditable="isEditable"
                    :echoData="echoData"
                    @changeEchoData="changeEchoData"
                    ref="opcsingleton"
                  ></opcConnector>
                </div>
              </template> -->
            </template>
          </template>
        </section>
      </template>

      <!--未分组显示根节点下的params，显示方式和groups一样-->
      <!-- <section class="ungrounded" v-if="dbsource[0].params"></section> -->
      <section
        v-if="tagName == 'mqtt' || tagName == 'kafka'"
        class="mqtt-config"
      >
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
      <section v-if="tagName == 'csv'">
        <CsvData
          :isEditable="isEditable"
          :echoData="echoData"
          ref="csvdata"
          @handleDbBtn="handleDbBtn"
        ></CsvData>
      </section>
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
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import marked from "marked";
import CsvData from "../components/csvData.vue";
import { debounce, deepClone } from "@/utils/index";
import { validPath } from "@/utils/validate";
import PThreeCheckbox from "../components/pThreeCheckbox.vue";
import MqttConnector from "../components/newMqttConnector.vue";
import opcConnector from "../components/opcConnector.vue";
import DialogCreateDb from "../components/addDbDialog.vue";
import Result from "../components/result.vue";
export default {
  name: "DbSourceUI",
  components: {
    "p-three-checkbox": PThreeCheckbox,
    MqttConnector,
    opcConnector,
    CsvData,
    DialogCreateDb,
    DataTarget,
    Result
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
    isCopyable: {
      type: Boolean,
    },
  },
  data() {
    return {
      allnodesloading: false,
      disableallnodeclick: true,
      opcinusefile: "",
      downloadUrl: process.env.VUE_APP_X_API + `/download?file_path=`,
      language: localStorage.getItem('local_language'),
      limit: 1,
      opcPointavalible: true,
      mqttcafile: [],
      mqttcertfile: [],
      mqttcertkeyfile: [],
      fileList: [],
      certfileList: [],
      privatefileList: [],
      uploadData: {
        req_id: new Date().getTime(),
      },
      opcfileList: [],
      fileurl: "",
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      openSSL: false,
      constmqttCols: [],
      textareas: ["ca", "cert", "cert_key"],
      styleobj: {
        width: "100%",
        display: "flex",
        //"align-items": "baseline",
        "margin-bottom": "8px",
      },
      styleareaobj: {
        width: "100%",
        display: "flex",
        "margin-bottom": "8px",
      },
      payloadVal: "",
      mqttpayload: ["json"],
      // dbsource,
      disable: false,
      address: "",
      port: "",
      username: "",
      password: "",
      subject: "",
      radio: "",
      dblist: [],
      dbprecision: "",
      isShowConfiguration: false,
      loading: false,
      configurationdata: [],
      activeDataSet: {},
      activeName: "",
      checkboxData: {
        label: "",
        disabled: false,
      },
      policyDisabled: true,
      isShowEditBtn: false,
      // dbsource: [],
      checkLoading: false,
      checkResult: {
        // valid: false,
        // support: false,
        // data_source: '',
        // version: '', // 返回数据源版本，不能获得版本则不返回该字段。
      },
      activeCollapse: ''
    };
  },
  created() {
    this.getDatabases();
    if (this.isEditable) {
      if (this.tagName == "mqtt") {
        this.payloadVal = "json";
      }
      if (this.dbsource[0].authentication) {
        this.dbsource[0].authentication.alternatives =
          this.dbsource[0].authentication.alternatives.map((item) => {
            if (item.name === "certificates") {
              item.params.map((par, index) => {
                if (par.name === "security_mode") {
                  this.policyDisabled = par.value && par.value === "None";
                  if (par.value && par.value !== "None") {
                    item.params[2].required = true;
                    item.params[3].required = true;
                  }
                }
                return par;
              });
            }
            return item;
          });
      }
      this.isShowEditBtn = this.isCopyable ? false : true;
    }
    console.log('dddd',this.checkResult,!this.checkResult.valid && !this.checkResult.support);

  },
  mounted() {
    if (this.tagName == "mqtt" || this.tagName == "kafka") {
      this.constmqttCols = this.dbsource[0].parser.fields;
      let caitem = this.$store.state.app.mqttcafile[0];
      let certitem = this.$store.state.app.mqttcertfile[0];
      let certkeyitem = this.$store.state.app.mqttcertkeyfile[0];
      if (
        caitem &&
        certitem &&
        certkeyitem &&
        caitem.length > 0 &&
        certitem.length > 0 &&
        certkeyitem.length > 0
      ) {
        this.mqttcafile = [].concat({
          name: caitem?.substr(caitem.lastIndexOf("/") + 1),
          percentage: 100,
          raw: File,
          response: [].concat(caitem),
          size: 87,
          status: "success",
          uid: 3,
        });
        this.mqttcertfile = [].concat({
          name: certitem?.substr(certitem.lastIndexOf("/") + 1),
          percentage: 100,
          raw: File,
          response: [].concat(certitem),
          size: 87,
          status: "success",
          uid: 4,
        });
        this.mqttcertkeyfile = [].concat({
          name: certkeyitem?.substr(certkeyitem.lastIndexOf("/") + 1),
          percentage: 100,
          raw: File,
          response: [].concat(certkeyitem),
          size: 87,
          status: "success",
          uid: 5,
        });
      }
    }

    if (this.tagName.includes("opc") && this.isEditable) {
      let certitem = this.$store.state.app.opccertfiles[0];
      let privateitem = this.$store.state.app.opcprivatefiles[0];
      if (certitem && privateitem) {
        this.certfileList = [].concat({
          name: certitem?.substr(certitem.lastIndexOf("/") + 1),
          percentage: 100,
          raw: File,
          response: [].concat(certitem),
          size: 87,
          status: "success",
          uid: 2,
        });
        this.privatefileList = [].concat({
          name: privateitem?.substr(privateitem.lastIndexOf("/") + 1),
          percentage: 100,
          raw: File,
          response: [].concat(privateitem),
          size: 87,
          status: "success",
          uid: 2,
        });
      }

      // if (flag) {
      // this.opcPointavalible = false;
      let item = this.$store.state.app.opcnodesfiles[0];
      this.opcinusefile = item;
      // this.opcfileList = [].concat({
      //   name: item?.substr(item.lastIndexOf("/") + 1),
      //   percentage: 100,
      //   raw: File,
      //   response: [].concat(item),
      //   size: 87,
      //   status: "success",
      //   uid: 1,
      // });
      // } else {
      //   // this.opcPointavalible = true;
      // }
    }

    this.activeName = this.dbsource[0].datasets
      ? this.dbsource[0].datasets.categories[0].category
      : "";
  },
  computed: {
    agentId() {
      return this.$store.state.app.currentAgentID || "";
    },
    sourceName() {
      return this.$store.state.app.currentDSName || ""
    },
    targetDatabase() {
      return this.$store.state.app.currentDBName || ""
    }
  },
  watch: {
    "$i18n.locale":{
      deep:true,
      handler(val){
        this.language=val
      }
    },
    "$store.state.app.currentDBName": {
      immediate: true,
      handler() {
        if (this.tagName == "kafka") {
          this.getdbprecision();
        }
      },
    },
    "$store.state.dbs.dialogDbVisible": {
      handler(val) {
        if (!val) {
          this.getDatabases();
        }
      },
    },
  },
  methods: {
    async downloadopcAllponits() {
      try {
        this.allnodesloading = true;
        this.disableallnodeclick = false;
        if (!this.dbsource[0].options.endpoint.value) {
          Message.error(this.$t("taoscluster.endpointRequired"));
          return;
        }
        let params = `${this.$store.state.app.currentDBType}://${this.dbsource[0].options.endpoint.value}&categories=nodes`;
        let result = await downlaodAllNodes(
          params,
          this.agentId
        );
        this.allnodesloading = false;
        this.disableallnodeclick = true;
        if (result && result.message) {
          Message.error(result.message);
          return;
        }

        let blob = new Blob([result], { type: "text/csv,charset=UTF-8" });
        let link = document.createElement("a");
        link.download = "list_of_all_nodes.csv";
        link.style.display = "none";
        link.href = URL.createObjectURL(blob);
        document.body.appendChild(link);
        link.click();
        URL.revokeObjectURL(link.href);
        document.body.removeChild(link);
      } catch (error) {
        this.this.allnodesloading = false;
        this.disableallnodeclick = true;
        console.log(error);
      }
    },
    handleopcSuccess(response, file, fileList) {
      this.opcfileList = [].concat(file);
    },
    handleopcRemove(file, filelist) {
      this.opcfileList = filelist;
    },
    changeOpcCollectMode(val) {
      if (this.tagName.includes("opc")) {
        let oldData = this.$store.state.app.opcConfig;
        let columnCons = [];
        if (val == "observe") {
          columnCons = oldData.column_configs.map((item) => {
            if (item.column_name == "received_ts") {
              item["is_primary_key"] = true;
            }
            if (item.column_name == "original_ts") {
              item["is_primary_key"] = false;
            }
            return item;
          });
          this.$store.commit("app/SET_OPC_CONFIG", {
            column_configs: columnCons,
            stable_prefix: oldData.stable_prefix,
          });
        } else {
          columnCons = oldData.column_configs.map((item) => {
            if (item.column_name == "received_ts") {
              item["is_primary_key"] = false;
            }
            if (item.column_name == "original_ts") {
              item["is_primary_key"] = true;
            }
            return item;
          });
        }
        this.$store.commit("app/SET_OPC_CONFIG", {
          column_configs: columnCons,
          stable_prefix: oldData.stable_prefix,
        });
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
    // getThreeBoxNum(val, item) {
    //   if (item.name == "use_csv_config") {
    //     if (val == 1) {
    //       this.opcPointavalible = false;
    //     } else {
    //       this.opcPointavalible = true;
    //     }
    //   }
    // },
    handleCertSuccess(response, file, fileList) {
      this.certfileList = [].concat(file)
    },
    handlePrivateSuccess(response, file, fileList) {
      this.privatefileList = [].concat(file)
    },
    handleSuccess(response, file, fileList) {
      this.fileList = [].concat(file)
    },
    handleMqttCaSuccess(response, file, fileList) {
      this.mqttcafile = [].concat(file)
    },
    handleMqttCertSuccess(response, file, fileList) {
      this.mqttcertfile = [].concat(file)
    },
    handleMqttCertKeySuccess(response, file, fileList) {
      this.mqttcertkeyfile = [].concat(file)
    },

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

    handleAuthentication(p) {
      if (p.name === "security_mode") {
        this.dbsource[0].authentication.alternatives =
          this.dbsource[0].authentication.alternatives.map((item) => {
            if (item.name === "certificates") {
              item.params.map((par) => {
                if (["certificate", "private_key"].includes(par.name)) {
                  par.required = p.value === "None" ? false : true;
                }
                if (par.name === "security_policy") {
                  this.policyDisabled = p.value === "None";
                  if (p.value === "None") {
                    par.value = "";
                  }
                }
                return par;
              });
            }
            return item;
          });
      }
    },

    async getdbprecision() {
      let res = await sendSQLReq(
        `select \`precision\` from information_schema.ins_databases where name = '${this.targetDatabase}';`
      );
      if (res && res.code == 0 && res.data[0]) {
        this.dbprecision = res.data[0][0];
      }
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
      // csv 不做检测
      this.checkResult = this.$options.data().checkResult
      this.submit(false)
    },
    // 数据源可用性和版本检查
    async getValidateResult(dns) {
      try {
        this.checkLoading = true
        let result = await validateTask(dns,this.agentId)
        console.log('result',result);
        this.checkResult = result
        this.checkLoading = false // 检测的 loading 效果
        this.activeCollapse = 'one'
      } catch (error) {
        this.checkLoading = false
        console.log('err');
      }
    },

    async submit(isSubmit) {
      debugger
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
        if (this.tagName != "csv" && isSubmit) {
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
        }
        if (!this.sourceName && isSubmit) {
          console.log('this.sourceName',this.sourceName);
          Message.warning(`${enterTip} ${this.$t('name')}`);
          return;
        }
        if (!this.targetDatabase && isSubmit) {
          Message.warning(`${enterTip} ${this.$t('stream.targetDB')}`);
          return;
        }

        if (data.authentication && data.authentication.value == "plain") {
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
        let reg = /\s+/g;
        dns = dns.replace(reg, "").trim();
        let querystr = "";
        if (data.groups && isSubmit) {
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
                if (this.tagName == "mqtt" || this.tagName == "kafka") {
                  if (data.groups[index].collapsed) {
                    if (
                      this.tagName == "mqtt" &&
                      this.mqttcafile.length > 0 &&
                      this.mqttcertfile.length > 0 &&
                      this.mqttcertkeyfile.length > 0
                    ) {
                      if (data.groups[index].params[g].name == "ca") {
                        querystr += `&${data.groups[index].params[g].name}=@${this.mqttcafile[0].response[0]}`;
                      }
                      if (data.groups[index].params[g].name == "cert") {
                        querystr += `&${data.groups[index].params[g].name}=@${this.mqttcertfile[0].response[0]}`;
                      }
                      if (data.groups[index].params[g].name == "cert_key") {
                        querystr += `&${data.groups[index].params[g].name}=@${this.mqttcertkeyfile[0].response[0]}&`;
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
                  if (
                    data.groups[index].params[g].name == "topics" &&
                    this.tagName == "mqtt"
                  ) {
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
                    // if (this.opcPointavalible) {
                    //   this.$refs.opcsingleton[0].submit();
                    //   if (this.$refs.opcsingleton[0].isReject) {
                    //     Message({
                    //       type: "warning",
                    //       message:
                    //         this.$t("datasource.msg") +
                    //         ":" +
                    //         `${data.groups[index].params[g].display} `,
                    //     });
                    //     return;
                    //   }
                    // }
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
                if (this.handleEmptyValue(data.groups[index].params[g].value)) {
                  if (data.groups[index].params[g].name === "use_received_time") {
                    if (data.groups[index].params[g].value !== 0) {
                      let value = data.groups[index].params[g].value === 1;
                      querystr +=
                        `${data.groups[index].params[g].name}=${value}` + "&";
                    }
                  } else if (data.groups[index].params[g].name === "path") {
                    if (!validPath(data.groups[index].params[g].value)) {
                      Message({
                        type: "warning",
                        message:
                          `${data.groups[index].params[g].display} ` +
                          ":" +
                          this.$t("formatWrong"),
                      });
                      return;
                    } else {
                      querystr +=
                        `${data.groups[index].params[g].name}=${data.groups[index].params[g].value}` +
                        "&";
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
                        if (
                          // data.groups[index].params[g].name == "debug" ||
                          data.groups[index].params[g].name == "use_csv_config"
                          // data.groups[index].params[g].name == "enable"
                        ) {
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
        }
        if (
          data.authentication &&
          data.authentication.value == "certificates"
        ) {
          for (
            let i = 0;
            i < data.authentication.alternatives[2].params.length;
            i++
          ) {
            let type = data.authentication.alternatives[2].params[i].hint.type;
            let authName = data.authentication.alternatives[2].params[i].name;

            let authValue =
              type == "file"
                ? authName == "certificate"
                  ? this.certfileList.length > 0
                    ? "@" + this.certfileList[0].response[0]
                    : ""
                  : this.privatefileList.length > 0
                  ? "@" + this.privatefileList[0].response[0]
                  : ""
                : data.authentication.alternatives[2].params[i].value;
            let authDisplay =
              data.authentication.alternatives[2].params[i].display;
            let authRequired =
              data.authentication.alternatives[2].params[i].required;
            if (authRequired && !authValue) {
              Message({
                type: "warning",
                message: this.$t("datasource.msg") + ":" + `${authDisplay} `,
              });
              return;
            } else {
              querystr += authValue ? `${authName}=${authValue}&` : "";
            }
          }
        }
        if (data.datasets && isSubmit) {
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
              if (this.tagName.includes("opc")) {
                console.log("无提示");
              } else {
                Message({
                  type: "warning",
                  message: `${enterTip} ${target.name} `,
                });
                return;
              }
            } else {
              //opc测点手动上传
              let dnsarr = querystr.split("&");
              let idx = dnsarr.findIndex((item) =>
                item.includes("csv_config_file=")
              );
              if (idx > -1) {
                dnsarr.splice(idx, 1);
                querystr = dnsarr.join("&");
              }
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
        if ((this.tagName == "mqtt" || this.tagName == "kafka") && isSubmit) {
          if (this.$refs.mqtt) {
            this.$refs.mqtt.submit();
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

        if (this.tagName.includes("opc") && isSubmit) {
          // if (this.opcPointavalible) {
          // let oldData = this.$store.state.app.opcConfig;
          // let columnCons = oldData.column_configs.filter((item) =>
          //   this.$parent.echoData.includes(item.column_name)
          // );
          // this.$store.commit("app/SET_OPC_CONFIG", {
          //   column_configs: columnCons,
          //   stable_prefix: oldData.stable_prefix,
          // });
          // let saveConf = {
          //   column_configs: columnCons,
          //   stable_prefix: oldData.stable_prefix,
          // };
          // let prefix = dns.split("?")[0];
          // let dnsarr = dns.split("?")[1].split("&");
          // let indx = dnsarr.findIndex((item) =>
          //   item.includes("opc_table_config=")
          // );
          // if (indx > -1) {
          //   dnsarr.splice(indx, 1);
          //   dns = prefix + "?" + dnsarr.join("&");
          // }
          // dns += "&opc_table_config=" + JSON.stringify(saveConf);
          // } else {
          if (this.dbsource[0].datasets.value == "csv_config_file") {
            if (
              this.opcfileList.length == 0 &&
              this.dbsource[0].datasets.value == "csv_config_file" &&
              !this.isEditable
            ) {
              Message({
                type: "warning",
                message: this.$t("datasource.uploadtip"),
              });
              return;
            }
            let prefix = dns.split("?")[0];
            let dnsarr = dns.split("?")[1].split("&");
            let ind = dnsarr.findIndex((item) =>
              item.includes("csv_config_file")
            );
            if (this.isEditable) {
              dnsarr.splice(
                ind,
                1,
                `&csv_config_file=@` +
                  (this.opcfileList.length > 0
                    ? this.opcfileList[0].response[0]
                    : this.opcinusefile)
              );
              dns = prefix + "?" + dnsarr.join("&");
              // dns += `&csv_config_file=@` + (this.opcfileList.lenght>0?this.opcfileList[0].response[0]:this.opcinusefile);
            } else {
              if (ind > -1) {
                dnsarr.splice(
                  ind,
                  1,
                  `&csv_config_file=@` + this.opcfileList[0].response[0]
                );
                dns = prefix + "?" + dnsarr.join("&");
              } else {
                dns += `&csv_config_file=@` + this.opcfileList[0].response[0];
              }
            }
          } else {
            let allStr = "";
            this.dbsource[0].datasets.categories[1].params.forEach(
              (item, index) => {
                allStr +=
                  `${item.name}=${item.value}` + (index <= 1 ? "&" : "");
              }
            );
            dns += "&" + allStr + `&select_all_points=true`;
          }

          // }
        }
        let piParams = {
          from:
            (this.tagName == "mqtt"
              ? "mqtt"
              : this.tagName == "csv"
              ? "csv"
              : this.tagName == "kafka"
              ? "kafka"
              : "opc" + this.protocol) + dns,
          name: this.sourceName,
          to:
            "taos+" +
            localStorage.getItem("base_url") +
            (this.targetDatabase
              ? "/" + this.targetDatabase
              : ""),
          labels: [
            "type::datain",
            `cluster-id::${id}`,
            `user::${localStorage.getItem("username")}`,
          ],
        };
        if (this.tagName == "mqtt" && isSubmit) {
          piParams["parser"] = this.$store.state.app.mqttParser;
        }
        if (this.tagName == "kafka" && isSubmit) {
          let value = this.$store.state.app.mqttParser.parse.payload;
          piParams["parser"] = {
            ...this.$store.state.app.mqttParser,
            parse: {
              value: {
                ...value,
                keep: false,
              },
              ts: {
                as: `timestamp(${this.dbprecision})`,
              },
            },
          };
        }
        if (this.agentId) {
          piParams["via"] = this.agentId;
        }
        if (this.tagName == "csv" && isSubmit) {
          this.$refs.csvdata.$refs.param.submit();
          this.$refs.csvdata.$refs.param.submit2();
          if (
            this.$refs.csvdata.activeName == "first" &&
            this.$refs.csvdata.fileList.length == 0
          ) {
            Message.error(this.$t("datasource.uploadcsvtip"));
            return;
          }
          if (
            this.$refs.csvdata.activeName == "second" &&
            !this.$refs.csvdata.fileurl
          ) {
            Message.error(this.$t("datasource.uploadcsvtip"));
            return;
          }
          if (!this.$refs.csvdata.$refs.param.isAllValid) {
            return;
          }
          if (!this.$refs.csvdata.$refs.csvconfig) {
            Message.error(this.$t("datasource.csvconfigtip"));
            return;
          }
          let model = this.$store.state.app.csvParser.model;
          let parse = this.$store.state.app.csvParser.parse;
          if (this.$store.state.app.csvtags.length > 0 && !model.tags) {
            model["tags"] = this.$store.state.app.csvtags;
          }
          if (model.tags && model.tags.length > 0) {
            model.name = this.$refs.csvdata.$refs.param.ruleForm2.subname;
            model.using = this.$refs.csvdata.$refs.param.ruleForm2.tableName;
            piParams["parser"] = this.$store.state.app.csvParser;
          } else {
            piParams["parser"] = Object.assign(
              { parse: parse },
              {
                model: {
                  name: this.$refs.csvdata.$refs.param.ruleForm2.subname,
                  columns: model.columns,
                },
              }
            );
          }

          if (model.columns.length == 0 || model.columns[0] == undefined) {
            Message.error(this.$t("datasource.csvwholeinfo"));
            return;
          }
          let flag = (
            model.tags ? [...model.columns, ...model.tags] : [...model.columns]
          ).some((item) => parse[item].as == "");
          if (flag) {
            Message.error(this.$t("datasource.csvwholeinfo"));
            return;
          }
          piParams["from"] =
            `csv:` +
            (this.$refs.csvdata.activeName == "first"
              ? this.$refs.csvdata.fileList.map((item, index) => {
                  return item.response[0];
                })
              : this.$refs.csvdata.fileurl) +
            dns.substring(3) +
            `&has_header=` +
            this.$refs.csvdata.$refs.param.ruleForm.hasHeader +
            (!this.$refs.csvdata.$refs.param.ruleForm.hasHeader
              ? `&header=${this.$refs.csvdata.$refs.param.ruleForm.customcol}`
              : "");
        }
        console.log(this.isEditable, this.editId, "编辑-opc");
        if (isSubmit) {
          if (this.isEditable && this.editId && !this.isCopyable) {
            let result = await EditSource(piParams, this.editId);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            this.$parent.changeEditable(false);
            this.$parent.toggleComponent("opctable", this.protocol);
          } else {
            let result = await AddSource(piParams);
            if (result.message) {
              Message.error(result.message);
              return;
            }
            if (result && result.id) {
              this.$parent.changeEditable(false);
              this.$parent.toggleComponent("opctable", "");
              Message.success(this.$t("datasource.successtip"));
            }
          }
        } else {
          this.getValidateResult(piParams.from)
        }
      } catch (err) {
        err.response &&
          err.response.data &&
          err.response.data.message &&
          Message.error(err.response.data.message);
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
      // let format = curData[0].id;
      let format = curData[0].format;
      format = format.replace("{id}", curData[0].id);
      let options = curData[0].options;
      for (let i = 0; i < options.length; i++) {
        if (options[i].required && !options[i].value) {
          Message({
            type: "warning",
            message: `${enterTip} ${options[i].display}`,
          });
          return;
        }
        // format += `::${options[i].value}`;
        if (format.indexOf(options[i].name) !== -1) {
          format = format.replace(`{${options[i].name}}`, options[i].value);
        }
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
    searchDatas: debounce(function (e, val) {
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
        if (
          data.authentication &&
          data.authentication.value == "certificates"
        ) {
          for (
            let i = 0;
            i < data.authentication.alternatives[2].params.length;
            i++
          ) {
            let type = data.authentication.alternatives[2].params[i].hint.type;
            let authName = data.authentication.alternatives[2].params[i].name;

            let authValue =
              type == "file"
                ? authName == "certificate"
                  ? this.certfileList.length > 0
                    ? "@" + this.certfileList[0].response[0]
                    : ""
                  : this.privatefileList.length > 0
                  ? "@" + this.privatefileList[0].response[0]
                  : ""
                : data.authentication.alternatives[2].params[i].value;
            let authDisplay =
              data.authentication.alternatives[2].params[i].display;
            let authRequired =
              data.authentication.alternatives[2].params[i].required;
            if (authRequired && !authValue) {
              Message({
                type: "warning",
                message: this.$t("datasource.msg") + ":" + `${authDisplay} `,
              });
              return;
            } else {
              querystr += authValue ? `${authName}=${authValue}&` : "";
            }
          }
        }
        if (data.authentication && data.authentication.value == "plain") {
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
          pattern: val,
          offset: 0,
          limit: 10,
        };
        if (this.agentId) {
          const viaObj = {
            via: this.agentId,
          };
          if (viaObj.via) {
            Object.assign(params, viaObj);
          }
        }

        this.loading = true;
        getUaAndDaData(params)
          .then((res) => {
            if (res && res.code && res.code != 0) {
              Message({
                type: "error",
                message: res && res.message,
              });
            } else {
              this.configurationdata = res;
              Message({
                type: "success",
                message: this.$t("operateSucc"),
              });
            }
            this.loading = false;
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
  font-weight: 500;
}
</style>
<style lang="scss" scoped>
.source-ui {
  justify-content: space-between;
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
  .left-ui.readable {
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
    .download_typefiles {
      position: relative;
      z-index: 999;
    }
  }
  .left-ui {
    position: relative;
    overflow: auto;
    width: 50%;
    flex-shrink: 0;

    .description {
      max-width: 568px;
      overflow: auto;
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
        color: red;
        font-size: 14px;
        line-height: 25px;
        margin-left: 4px;
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
    margin-bottom: 0px !important;
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
      position: relative;
      .searchListItem {
        border-bottom: 1px solid #dcdfe6;
        line-height: 30px;
        padding-left: 5px;
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
  .opcconf {
    &.notallowed {
      position: relative;
      display: none;
      // &::after {
      //   content:'';
      //   position: absolute;
      //   top: 0;
      //   bottom: 0;
      //   left: 0;
      //   right: 0;
      //   background: #f2f6fc40;
      //   z-index:9999;
      // }
    }
  }
  .groups-dataset.notallowed {
    display: none;
  }
  .groups.tableconfig.notallowed {
    display: none;
  }
  .cancel-btn,
  .edit-btn {
    z-index: 101;
  }
}
.allnodes {
  display: inline-block;
  margin-left: 20px;
  color: #4259ce;

  &.noclick {
    cursor: not-allowed;
    pointer-events: none;
    color: #acaab2;
  }
  &.click {
    cursor: pointer;
  }
}
.upload-demo {
  display: flex;
  align-items: baseline;
}
::v-deep {x
  .el-upload-list__item {
    margin-top: 1px !important;
  }
  .el-upload-list__item-name{
    max-width:120px;
  }
}
</style>
