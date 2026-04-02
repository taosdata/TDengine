<template>
  <div class="topic-sample">
    <el-tabs v-model="activeLang" type="card" class="topic-tab" @tab-click="changeLang">
      <el-tab-pane class="topic-go" name="go" :label="'Go'">
        <docs :category="'topic'" :lang="'Go'" :topic="topicTitle"></docs>
      </el-tab-pane>
      <el-tab-pane class="topic-rust" name="rust" :label="'Rust'">
        <docs :category="'topic'" :lang="'Rust'" :topic="topicTitle"></docs>
      </el-tab-pane>
      <el-tab-pane class="topic-python" name="python" :label="'Python'">
        <docs :category="'topic'" :lang="'Python'" :topic="topicTitle"></docs>
      </el-tab-pane>
      <el-tab-pane class="topic-java" name="java" :label="'Java'">
        <docs :category="'topic'" :lang="'Java'" :topic="topicTitle"></docs>
      </el-tab-pane>
    </el-tabs>
    <!-- <el-form inline class="topic-example-select">
      <el-form-item :label="$t('topic.topic')">
        <el-select v-model="currentTopic" placeholder="Topic Select" @change="$emit('change')">
          <el-option v-for="item in topicList" :key="item.topicId" :label="item.topicName" :value="item.topicId"></el-option>
        </el-select>
      </el-form-item>
    </el-form> -->
    <div class="topic-example-select">
      <label class="topic-title">{{ $t('topic.topic') }}</label>
      <el-select
        v-model="currentTopic"
        class="topic-select-content"
        placeholder="Topic Select"
        @change="$emit('change')"
      >
        <el-option v-for="item in topicList" :key="item" :label="item" :value="item"></el-option>
      </el-select>
    </div>
  </div>
</template>

<script>
import { sendSQLReq } from '@/api/explorer';
import Docs from '@/components/document/docs.vue';
export default {
  components: { Docs },
  props: {},
  emits: ['change'],
  data() {
    return {
      currentTopic: '',
      activeLang: 'go',
      topicList: [],
      mainEl: null,
      exTabEl: null,
      topicSelEl: null,
      topicSelTop: 0,
      currentPage: 1,
      pageSize: 10,
      langFixed: {
        go: {
          fixed: false,
          scrollTop: 0
        },
        rust: {
          fixed: false,
          scrollTop: 0
        },
        python: {
          fixed: false,
          scrollTop: 0
        },
        java: {
          fixed: false,
          scrollTop: 0
        }
      }
    };
  },
  computed: {
    topicTitle() {
      const foundItem = this.topicList.find(item => {
        return item === this.currentTopic;
      });
      return foundItem ? foundItem : '';
    }
  },
  created() {
    this.getTopicList();
  },
  mounted() {
    const tmpEl = document.querySelector('.main-content');
    if (tmpEl) {
      this.mainEl = tmpEl;
      this.mainEl.addEventListener('scroll', this.handleScroll);
      const tmpEl1 = tmpEl.querySelector('.topic-tab');
      if (tmpEl1) {
        this.exTabEl = tmpEl1;
      }
      // const tmpEl2 = tmpEl.querySelector(".topic-example-select");
      // if (tmpEl2) {
      //   this.topicSelEl = tmpEl2;
      //   this.topicSelTop = this.topicSelEl.offsetTop;
      // }
    }
  },
  beforeUnmount() {
    this.element?.removeEventListener('scroll', this.handleScroll);
  },
  methods: {
    // async init() {
    //   await this.getTopics({ currentPage: this.currentPage, pageSize: this.pageSize });
    //   if (this.$route.query?.topicId) {
    //     this.currentTopic = this.$route.query.topicId;
    //   }
    // },
    async getTopicList() {
      try {
        await sendSQLReq(`show topics;`)
          .then(res => {
            this.topicList = res.data.map(data => {
              return data.join('');
            });
            this.currentTopic = this.topicList[0];
          })
          .catch(err => {
            // err.desc && this.$error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
      }
    },
    changeLang() {
      if (!this.mainEl) {
        return;
      }
      let foundFixed;
      let maxTop = 0;
      for (const langKey of Object.keys(this.langFixed)) {
        const langItem = this.langFixed[langKey];
        if (langItem.fixed) {
          foundFixed = true;
        }
        if (maxTop < langItem.scrollTop) {
          maxTop = langItem.scrollTop;
        }
      }
      let mainTop;
      if (!foundFixed) {
        mainTop = maxTop;
      } else {
        const tmpScrollTop = this.langFixed[this.activeLang].scrollTop;
        if (tmpScrollTop <= 150) {
          mainTop = 150;
        } else {
          mainTop = tmpScrollTop;
        }
      }
      this.mainEl.scrollTo({
        top: mainTop
      });
    },
    handleScroll() {
      if (!this.exTabEl || !this.mainEl) {
        return;
      }
      const acLang = this.activeLang;
      const viewWrap = this.exTabEl.querySelector(`.topic-${acLang} `);
      const viewWrapWith = viewWrap.clientWidth - 60;
      const viewEl = this.exTabEl.querySelector(`.topic-${acLang} .view-header`);
      if (!viewEl) {
        return;
      }
      if (!this.langFixed[acLang].fixed && this.mainEl.scrollTop > 160 && viewEl?.style?.position !== 'fixed') {
        viewEl.style.position = 'fixed';
        viewEl.style.top = '5.8rem';
        viewEl.style.width = `${viewWrapWith}px`;
        viewEl.style['z-index'] = '900';
        this.langFixed[acLang].fixed = true;
      } else if (this.langFixed[acLang].fixed && viewEl.style.position !== 'relative' && this.mainEl.scrollTop <= 160) {
        viewEl.style.position = 'relative';
        viewEl.style.top = '0';
        viewEl.style.width = '100%';
        viewEl.style['z-index'] = '0';
        this.langFixed[acLang].fixed = false;
      }
      this.langFixed[acLang].scrollTop = this.mainEl.scrollTop;
      if (!this.topicSelEl) {
        return;
      }
      if (this.mainEl.scrollTop < 105) {
        this.topicSelEl.style.top = this.topicSelTop - this.mainEl.scrollTop + 'px';
      }
    }
  }
};
</script>

<style scoped lang="scss">
.topic-sample {
  position: relative;

  .topic-sticky {
    position: sticky;
    top: 7rem;
    z-index: 1000;
  }

  .topic-tab {
    :deep(.el-tabs__header) {
      position: sticky;
      top: 3rem;
      z-index: 1000;
      background-color: white;
    }

    :deep(.tab-python),
    :deep(.doc-config-tab) {
      .el-tabs__header {
        z-index: unset;
      }
    }

    :deep(#tab-python.is-active),
    :deep(#tab-go.is-active),
    :deep(#tab-rust.is-active),
    :deep(#tab-java.is-active) {
      font-weight: 600;
      color: white;
      background-color: #4259ce;
    }

    :deep(.view-header) {
      width: 100%;
    }
  }

  .topic-title {
    margin-right: 1rem;
  }

  .topic-example-select {
    // position: fixed;
    // top: 22rem;
    // right: 5.5rem;
    position: absolute;
    top: -5px;
    right: 0;
    z-index: 1000;
    background-color: white;

    .topic-select-content {
      width: 200px;

      :deep(.el-input__inner) {
        height: 35px;
      }
    }
  }
}
</style>
