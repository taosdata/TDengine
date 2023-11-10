<template>
  <section>
    <section class="tab-block-first">
      <section class="flexStart">
        <div class="left">
          <el-select
            class="w100"
            v-if="config.multiple"
            v-model="value"
            size="mini"
            :multiple="true"
            :allow-create="config.editable"
            placeholder=""
            :disabled="disabled"
            filterable
            default-first-option
          >
            <el-option
              v-for="(t, tind) in value"
              :key="tind"
              :value="tind"
              :label="t"
              disabled
            >
            </el-option>
          </el-select>
          <el-input
            v-else
            size="mini"
            :disabled="disabled"
            class="w100"
            v-model="value"
          ></el-input>
        </div>
        <el-button
          v-if="!regexShow"
          size="mini"
          @click="regexShow = true"
          :disabled="disabled"
          >{{ $t('select') }}</el-button
        >
      </section>
      <div
        v-show="regexShow"
        class="left mt20"
      >
        <el-input
          v-model="regex"
          @keyup.enter.native="getData"
          size="mini"
          :disabled="requestIng"
          :placeholder="$t('dataIn.regexPatternInput')"
        >
          <el-button
            slot="append"
            :disabled="requestIng"
            @click="getData"
            icon="el-icon-search"
          ></el-button>
        </el-input>
        <el-collapse
          v-if="result.length || requestIng"
          v-loading="requestIng"
          class="m20 collapse-wrapper"
          v-model="activeName"
          accordion
        >
          <el-collapse-item
            v-for="(item, index) in result"
            :key="item.id"
            :title="item.id"
            :name="item.id"
          >
            <el-form
              :model="item"
              ref="option-form"
              size="mini"
              label-width="auto"
            >
              <template v-if="item.options">
                <el-form-item
                  v-for="(ite, index) in item.options"
                  :key="ite.name"
                  :label="ite.name"
                  :prop="'options.' + index + '.value'"
                  :required="ite.required"
                >
                  <el-input v-model="ite.value"> </el-input>
                </el-form-item>
              </template>

              <el-form-item>
                <el-button
                  class="w100"
                  type="primary"
                  @click="addOption(item, index)"
                  >{{ $t('add') }}</el-button
                >
              </el-form-item>
            </el-form>
            <!-- <ul
              class="option-list"
              v-if="item.options"
            >
              <li
               
                >
                <tempalte slot="prepend">
                  {{ ite.name }}
                </tempalte>
              </li>
            </ul> -->
          </el-collapse-item>
        </el-collapse>
        <!-- <ul class="result-list">
          <li
            v-for="item in result"
            class="detail-display"
            :key="item.id"
          >
            <span class="title">{{ item.id }}:</span>
            <span class="value">{{ item.options.join(',') }}</span>
          </li>
        </ul> -->
      </div>
    </section>
  </section>
</template>

<script>
import { getDataSetDefinitions } from '@/api/dataSource.js';
import { getDataSetDsn } from '../utils';
export default {
  props: {
    data: {
      type: Object,
      default: () => ({})
    },
    config: {
      type: Object,
      default: () => ({})
    },
    disabled: {
      type: Boolean,
      default: false
    }
  },
  inject: ['sourceParent'],
  components: {},
  data() {
    return {
      regex: '',
      regexShow: false,
      requestIng: false,
      activeName: '',
      result: []
    };
  },
  computed: {
    value: {
      get() {
        if (this.config.multiple) {
          return this.data[this.config.field] ? this.data[this.config.field]?.split(',') : [];
        } else {
          return this.data[this.config.field];
        }
      },
      set(val) {
        if (this.config.multiple) {
          this.data[this.config.field] = val.join(',');
        } else {
          this.data[this.config.field] = val;
        }
      }
    },
    isEdit() {
      return this.sourceParent.isEdit;
    },
    sourceType() {
      return this.sourceParent.sourceForm.type;
    }
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {
    getData() {
      if (this.requestIng) return;
      this.requestIng = true;
      const params = {
        categories: [this.config.category],
        from: this.sourceType + getDataSetDsn(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition),
        offset: 0,
        limit: 10,
        pattern: this.regex
      };
      if (this.sourceParent.sourceForm.agent) {
        params.via = this.sourceParent.sourceForm.agent;
      }
      getDataSetDefinitions(params)
        .then(data => {
          this.result = data;
        })
        .catch(() => {
          this.result = [];
        })
        .finally(() => {
          this.requestIng = false;
        });
    },
    addOption(result, index) {
      this.$refs['option-form']?.[index].validate(valid => {
        if (valid) {
          const { options, id, format } = result;
          let value = id ?? '';
          if (options) {
            if (!id && format) {
              value = options.reduce((pre, cur) => {
                return pre.replace(`{${cur.name}}`, cur.value);
              }, format);
            } else {
              value = id + '::' + options.map(item => item.value).join('::');
            }
          }

          if (this.config.multiple) {
            this.value = [...new Set([...this.value, value])];
          } else {
            this.value = value;
          }
        }
      });
    }
  }
};
</script>

<style scoped lang="scss">
.tab-block-first {
  .left {
    width: 300px;
    margin-right: 20px;
  }
}
.collapse-wrapper {
  max-height: 200px;
  overflow-y: auto;
}
.option-list li + li {
  margin-top: 10px;
}
</style>
