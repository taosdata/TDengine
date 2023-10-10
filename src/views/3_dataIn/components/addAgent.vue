<template>
  <section class="markdown-body">
    <el-steps
      :active="active"
      finish-status="success"
    >
      <el-step :title="$t('dataIn.downloadInstall')"> </el-step>
      <el-step :title="$t('generateToken')"></el-step>
      <el-step :title="$t('configure')"></el-step>
      <el-step :title="$t('dataIn.runAgent')"></el-step>
    </el-steps>
    <section v-if="active == 1">
      <p v-dompurify-html="$t('docs.taosxAgent.1', downloadUrl)"></p>
      <pre v-highlight><code>taosx-agent -V</code></pre>
    </section>
    <section v-else-if="active == 2">
      <p v-dompurify-html="$t('docs.taosxAgent.2')"></p>
      <el-input
        v-model="name"
        maxlength="32"
        size="mini"
        :placeholder="$t('pInName')"
        class="mb10"
      ></el-input>
      <p
        v-if="nameValid()"
        class="errorText"
      >
        {{ $t('dataIn.agentNameExist', [this.name]) }}
      </p>
    </section>
    <section v-else-if="active == 3">
      <p v-dompurify-html="$t('docs.taosxAgent.3')"></p>
      <pre v-highlight><code>endpoint="{{ taoxAddress }}"
token="{{ token }}"</code></pre>
      <p v-dompurify-html="$t('docs.taosxAgent.6')"></p>
    </section>
    <section
      v-else-if="active == 4"
      class="step4"
    >
      <p v-dompurify-html="$t('docs.taosxAgent.4')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane
          label="Linux"
          name="0"
        >
          <pre v-highlight><code>systemctl start taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane
          label="Windows"
          name="1"
        >
          <pre v-highlight><code>sc start taosx-agent</code></pre>
        </el-tab-pane>
      </el-tabs>
      <p v-dompurify-html="$t('docs.taosxAgent.5')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane
          label="Linux"
          name="0"
        >
          <pre v-highlight><code>systemctl status taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane
          label="Windows"
          name="1"
        >
          <pre v-highlight><code>sc query taosx-agent</code></pre>
        </el-tab-pane>
      </el-tabs>
      <!-- <p v-dompurify-html="$t('docs.taosxAgent.7')"></p>
      <el-tabs v-model="tabActive">
        <el-tab-pane
          label="Linux"
          name="0"
        >
          <pre v-highlight><code>journalctl -u taosx-agent</code></pre>
        </el-tab-pane>
        <el-tab-pane
          label="Windows"
          name="1"
        >
          <pre v-highlight><code>C:\Program Files\taosX\log\agent\</code></pre>
        </el-tab-pane>
      </el-tabs>
      <p
        style="margin-bottom: 16px"
        v-dompurify-html="$t('docs.taosxAgent.8')"
      ></p> -->
      <el-button
        class="mb20"
        size="small"
        @click="checkAgentStatus"
        :type="checkBtnType"
        >{{ checkBtnText }}</el-button
      >
      <template v-if="agentStatus == 'failed'">
        <p v-dompurify-html="$t('docs.taosxAgent.11')"></p>
        <el-tabs v-model="tabActive">
          <el-tab-pane
            label="Linux"
            name="0"
          >
            <pre v-highlight><code>journalctl -u taosx-agent</code></pre>
          </el-tab-pane>
          <el-tab-pane
            label="Windows"
            name="1"
          >
            <pre v-highlight><code>C:\Program Files\taosX\log\agent\</code></pre>
          </el-tab-pane>
        </el-tabs>
        <p
          style="margin-bottom: 16px"
          v-dompurify-html="$t('docs.taosxAgent.12')"
        ></p>
      </template>
    </section>
    <section class="flexCenter">
      <el-button
        :disabled="active == 1"
        size="mini"
        @click="active--"
        >{{ $t('prev') }}</el-button
      >
      <el-button
        size="mini"
        type="primary"
        :loading="loading"
        @click="next"
        :disabled="nextButton"
        >{{ nextButtonText }}</el-button
      >
    </section>
  </section>
</template>

<script>
import 'github-markdown-css/github-markdown-light.css';
import { OfficialSite } from '@/const';
import {
  addNewAgent,
  editAgent,
} from "@/api/explorer/agent";
export default {
  props: {
    agent: {
      type: Object,
      default: () => {}
    }
  },
  name:'AddAgent',
  components: {},
  data() {
    return {
      active: 1,
      name: '',
      tokenMap: {},
      loading: false,
      tabActive: '0',
      agentStatus: 'noCheck',
      requestIng: false
    };
  },
  computed: {
    checkBtnText() {
      return this.$t(
        'docs.taosxAgent.' +
          {
            noCheck: '7',
            checking: 10,
            success: 8,
            failed: 9
          }[this.agentStatus]
      );
    },
    checkBtnType() {
      return {
        noCheck: '',
        checking: 'primary',
        success: 'success',
        failed: 'danger'
      }[this.agentStatus];
    },
    agentList() {
      return this.$store.state.app.agentLists.filter(item => item.id !== this.agent?.id);
    },
    downloadUrl() {
      const assetsUrl = OfficialSite + '/assets-download/3.0/taosx-agent-1.2.4-';
      return {
        linuxDL: assetsUrl + 'linux-x64.tar.gz',
        windowDL: assetsUrl + 'windows-x64-installer.exe'
      };
    },
    nameError() {
      return this.nameValid();
    },
    token() {
      return this.tokenMap[this.name] ?? '';
    },
    taoxAddress() {
      return localStorage.getItem("local_endpoint") ?? '';
    },
    nextButton() {
      if (this.loading) return true;
      if (this.active == 2) {
        if (!this.name || this.nameError) {
          return true;
        }
      }
      return false;
    },
    nextButtonText() {
      return this.active == 4 ? this.$t('dataIn.finish') : this.$t('next');
    }
  },
  watch: {
    agent: {
      handler(val) {
        if (val) {
          this.name = val.name;
          this.active = 2;
        }
      },
      deep: true,
      immediate: true
    }
  },
  created() {},
  mounted() {},
  methods: {
    checkAgentStatus() {
      if (this.requestIng) return;
      this.requestIng = true;
      this.agentStatus = 'checking';
      this.$store
        .dispatch('app/getAgentList')
        .then(() => {
          const status = this.$store.state.app.agentLists.find(item => item.name == this.name)?.status;
          this.agentStatus = ['idle', 'busy'].includes(status) ? 'success' : 'failed';
        })
        .catch(() => {
          this.agentStatus = 'noCheck';
        })
        .finally(() => {
          this.requestIng = false;
        });
    },
    submit() {
      if (this.loading) return;
      this.loading = true;
      const fn = this.agent?.id ? editAgent : addNewAgent;
      fn(this.name, this.agent?.id)
        .then(({ token }) => {
          this.$set(this.tokenMap, this.name, token);
          this.active++;
          this.$store.dispatch('app/getAgentList');
        })
        .catch(() => {})
        .finally(() => {
          this.loading = false;
        });
    },
    next() {

      if (this.active == 4) {
        this.$parent.$parent.showAgent=false
      }
      // return this.$store.commit('SET_DIALOG_VISIBLE', false);
      if (this.active == 2) {
        this.submit();
      } else {
        this.active++;
      }
    },
    nameValid() {
      if (this.name) {
        return this.agentList.some(item => item.name == this.name);
      } else {
        return false;
      }
    }
  }
};
</script>

<style scoped lang="scss">
.markdown-body {
  padding: 0;
  word-break: break-word;
  .step4 p {
    margin-bottom: 0;
  }
  p {
    line-height: 24px;
  }
  .mb10{
    margin-bottom:10px;
  }
  .mb20{
    margin-bottom:20px;
  }
}
</style>
