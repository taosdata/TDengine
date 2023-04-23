<template>
  <div class="source-ui">
    <div class="left-ui">
      <section class="header">
        <h1>{{ dbsource[0].name ? dbsource[0].name : "" }}</h1>

        <!-- <h3>{{ dbsource[0].description }}</h3> -->
      </section>
      <section class="basics">
        <div class="block-title">
          <span>{{ dbsource[0].options.display }}</span>
        </div>
        <div class="protocol" v-if="dbsource[0].protocol">
          <span class="label">{{ dbsource[0].protocol.display }}</span>
          <div class="label-value">
            <el-select
              v-model="dbsource[0].protocol.value"
              placeholder="Please select protocol"
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
                oninput="value=>value.replace()"
                :placeholder="dbsource[0].options.host.placeholder"
              ></el-input>
              <div
                v-html="transforHtml(dbsource[0].options.host.description)"
                class="description"
              ></div>
            </div>
          </div>
          <div style="width: 100%" v-if="dbsource[0].options.port&&dbsource[0].options.port.display">
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
              ></el-input>
              <div
                v-html="transforHtml(dbsource[0].options.port.description)"
                class="description"
              ></div>
            </div>
          </div>
        </div>
        <!-- <div style="width: 100%">
          <span
            :class="[
              'label',
              dbsource[0].options.username.required ? 'required' : '',
            ]"
            >{{ dbsource[0].options.username.display }}</span
          >
          <div class="label-value">
            <el-input
              :placeholder="dbsource[0].options.username.placeholder"
              v-model="dbsource[0].options.username.value"
            ></el-input>
            <div
              v-html="transforHtml(dbsource[0].options.username.description)"
              class="description"
            ></div>
          </div>
        </div> -->
        <!-- <div style="width: 100%">
          <span
            :class="[
              'label',
              dbsource[0].options.password.required ? 'required' : '',
            ]"
            >{{ dbsource[0].options.password.display }}</span
          >
          <div class="label-value">
            <el-input
              :placeholder="dbsource[0].options.password.placeholder"
              v-model="dbsource[0].options.password.value"
              type="password"
            ></el-input>
            <div
              v-html="transforHtml(dbsource[0].options.password.description)"
              class="description"
            ></div>
          </div>
        </div> -->
        <div style="width: 100%">
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
            ></el-input>
            <div
              v-html="transforHtml(dbsource[0].options.subject.description)"
              class="description"
            ></div>
          </div>
        </div>
      </section>
      <section class="authentication" v-if="dbsource[0].authentication&&dbsource[0].authentication.display">
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
          <el-radio-group v-model="dbsource[0].authentication.value">
            <template v-for="at in dbsource[0].authentication.alternatives">
              <el-radio :key="at.name" :label="at.name"
                >{{ at.display }}
                <span class="des" style="color: #acaab2" v-if="at.description"
                  >({{ at.description }})</span
                >
              </el-radio>
            </template>
          </el-radio-group>
          <div class="authen-details">
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
                v-for="al in dbsource[0].authentication.alternatives.slice(1)"
                :key="al.name"
                style="display: flex; align-items: baseline"
              >
                <span class="label">{{ al.display }}</span>
                <div
                  v-for="(p, index) in al.params"
                  :key="index"
                  style="width: 100%"
                >
                  <el-input v-model="p.value"></el-input>
                  <div
                    class="description"
                    v-html="transforHtml(p.description)"
                  ></div>
                </div>
              </div>
            </template>
          </div>
        </div>
      </section>
      <template v-for="item in dbsource[0].groups">
        <section :class="['groups', item.name]" :key="item.display_order">
          <div style="flex-direction: column; align-items: baseline">
            <div class="block-title">
              <span>{{ item.name }}</span>
            </div>
            <div
              class="description"
              v-html="transforHtml(item.description)"
            ></div>
          </div>
          <template v-for="(p,pind) in item.params">
            <div :key="pind">
              <span :class="['label', p.required ? 'required' : '']">
                {{ p.display ? p.display : p.name }}
              </span>
              <div class="label-value">
                <template v-if="p.hint === 'str' || p.hint === 'timeout'">
                  <el-input
                    v-model="p.value"
                    placeholder="Please enter "
                  ></el-input>
                </template>
                <template v-if="p.hint.type && p.hint.type === 'str'">
                  <template v-if="p.hint.choices">
                    <el-select
                      v-model="p.value"
                      placeholder="Please select"
                      style="margin-left: -15px"
                    >
                      <el-option
                        v-for="c in p.hint.choices"
                        :key="c"
                        :label="c"
                        :value="c"
                      ></el-option>
                    </el-select>
                  </template>
                  <el-input v-else v-model="p.value"></el-input>
                </template>
                <template v-if="p.hint === 'bool'">
                  <el-radio-group v-model="p.value">
                    <el-radio v-for="c in p.choices" :key="c" :label="c">
                      {{ c }}
                    </el-radio>
                  </el-radio-group>
                </template>
                <template
                  v-if="
                    (p.hint.type && p.hint.type === 'integer') ||
                    p.hint === 'integer'
                  "
                >
                  <el-input-number v-model="p.value"></el-input-number>
                </template>
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
        <span class="label">Target Database</span>
        <el-select
          v-model="dbname"
          placeholder="Please select"
          style="margin-left: -15px"
        >
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
import { AddSource, EditSource } from "@/api/explorer/datain";
import { Message } from "element-ui";
import marked from "marked";
import { decrypt } from "@/utils/index";
export default {
  name: "DbSourceUI",
  props: {
    tagName: {
      type: String,
      default: "datasource",
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
  filters:{
    transtozh(val){
    }
  },
  data() {
    return {
      decryptPwd: "", //解密的密码
      //   dbsource,
      disable: false,
      address: "",
      port: "",
      username: "",
      password: "",
      subject: "",
      radio: "",
      dblist: [],
      dbname: "",
    };
  },
  created() {
    this.getDatabases();
    if (this.isEditable) {
      this.dbname = this.dbName;
    }
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
              message: `Please enter ${data.options[key].display} `,
            });
            return;
          }
        }
        this.decryptPwd = decrypt(localStorage.getItem("pwd"));
        if (this.tagName === "datasource") {
          dns += `://${localStorage.getItem("username")}:${this.decryptPwd}@${
            data.options.host.value ? data.options.host.value : ""
          }
        `;
        }else{
          dns +=`://${
            data.options.host.value ? data.options.host.value : ""
          }`
        }

        if (data.options.port) {
          dns +=
            ((Object.is(data.options.port.value, null)||!data.options.port.value) ? "" : ":") +
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
                message: `Please enter ${data.groups[index].params[g].name} `,
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
          name: localStorage.getItem("datainName"),
          to:
            "taos+" +
            localStorage.getItem("base_url") +
            (this.dbname ? "/" + this.dbname : ""),
          labels: ["type::datain", `cluster-id::${id}`],
        };
        
        if (this.tagName === "datasource") {
          if (this.isEditable) {
            await EditSource(apiParams, this.editId).then(() => {
              this.$parent.toggleComponent("tmqtable");
            }).catch(err=>{
              err.response.data.message&&Message.error(err.response.data.message)
            });
          } else {
            await AddSource(apiParams).then((res) => {
              this.$parent.toggleComponent('tmqtable');
            }).catch(err=>{
              err.response.data.message&&Message.error(err.response.data.message)
            });
          }
        } else {
          let piParams = {
            from: "pi" + dns,
            name: localStorage.getItem("datainName"),
            //   + (data.protocol?(Object.is(data.protocol.value, "--") ? "" : "+"):'') + dns,
            // name: localStorage.getItem("datainName"),
            to:
              "taos+" +
              localStorage.getItem("base_url") +
              (this.dbname ? "/" + this.dbname : ""),
            labels: ["type::datain", `cluster-id::${id}`],
          };
          if (this.isEditable) {
            await EditSource(piParams, this.editId).then(() => {
              this.$parent.toggleComponent("pitable");
            });
          } else {
            await AddSource(piParams).then((res) => {
              if (res && res.id) {
              this.$parent.toggleComponent('pitable');
              Message.success("Operation Successfully!");
            }
            });
          }
          // await AddSource(piParams).then((res) => {
          //   if (res && res.id) {
          //     this.$parent.toggleComponent('pitable');
          //     Message.success("Operation Successfully!");
          //   }
          // });
        }
      } catch (error) {
        console.log(error);
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
  .label-value {
    display: flex;
    flex-direction: column;
    // max-width: 500px;
    color: #acaab2;
    white-space: pre-wrap;
  }
  .left-ui {
    section:not(:first-child) {
      border: 1px solid #e3e4e6;
      margin-bottom: 20px;
      border-radius: 12px;
      padding: 15px;
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
    .label.required {
      position: relative;
      &::before {
        content: "*";
        position: absolute;
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
}
</style>
