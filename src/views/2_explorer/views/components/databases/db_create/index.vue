<template>
  <div class="dbCreate">
    <div class="dbCreate_title">{{ formTitle }}</div>
    <div class="formWrapper">
      <el-form class="form_style1" label-position="left" label-width="230px" ref="dbForm1" :rules="rules" :model="db_form">
        <el-form-item :label="$t('data.name')" prop="name">
          <el-input :disabled="isEdit" maxlength="32" size="small" v-model="db_form.name" />
        </el-form-item>
      </el-form>
      <div class="section2">
        <div class="sub_title">{{ $t("data.configParams") }}</div>
        <el-form size="small" class="form_style_2col" label-position="left" label-width="230px" :model="db_form" :rules="rules">
          <el-collapse v-model="activeNames">
            <el-collapse-item :title="$t('data.performanceRelatedParameters')" name="1">
              <div class="column1">
                <!-- BUFFER -->
                <el-form-item>
                  <span slot="label">
                    BUFFER
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.bufferTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.buffer"
                    :min="3"
                    :max="16384"
                    controls-position="right"
                    class="form_item"
                    placeholder="96MB"
                  >
                  </el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    TSDB_PAGESIZE
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.tsdbPagesizeTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.tsdb_pagesize"
                    :min="1"
                    :max="16384"
                    controls-position="right"
                    class="form_item"
                    placeholder="4KB"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>
                <!-- CACHEMODEL -->
                <el-form-item>
                  <span slot="label">
                    CACHEMODEL
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.cacheModelTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-select v-model="db_form.cachemodel" placeholder="none" class="w100" style="width: 130px">
                    <el-option value="none"></el-option>
                    <el-option value="last_row"></el-option>
                    <el-option value="last_value"></el-option>
                    <el-option value="both"></el-option>
                  </el-select>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    PAGESIZE
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.pageSizeTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.pagesize"
                    :disabled="isEdit"
                    :min="1"
                    :max="16384"
                    controls-position="right"
                    class="form_item"
                    placeholder="4kb"
                  ></el-input-number>
                </el-form-item>
              </div>
              <div class="column2">
                <el-form-item>
                  <span slot="label">
                    PAGES
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.pagesTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.pages"
                    :min="64"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="256"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    VGROUPS
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.vgroupsTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.vgroups"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="2"
                  ></el-input-number>
                </el-form-item>  
                <el-form-item>
                  <span slot="label">
                    STT_TRIGGER
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.sttTaiggerTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.stt_trigger"
                    :min="1"
                    :max="16"
                    controls-position="right"
                    class="form_item"
                    placeholder="1"
                  ></el-input-number>
                </el-form-item> 
                 <!-- CACHESIZE -->
                 <el-form-item>
                  <span slot="label">
                    CACHESIZE
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.cacheSizeTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number size="small" v-model="db_form.cachesize" :min="1" :max="65536" controls-position="right" class="form_item" placeholder="1MB">
                  </el-input-number>
                </el-form-item>                   
              </div>
            </el-collapse-item>
            <el-collapse-item :title="$t('data.dataPersistenceParameters')" name="2">
              <div class="column1">
                <el-form-item>
                  <span slot="label">
                    DURATION
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.durationTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input v-model="db_form.duration" :disabled="isEdit" controls-position="right" class="form_item" style="width: 130px" placeholder="50d"></el-input>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    MINROWS
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.minRowsTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.minrows"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="100"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    COMP
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.compTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    :disabled="isEdit"
                    v-model="db_form.comp"
                    :min="0"
                    :max="2"
                    controls-position="right"
                    class="form_item"
                    placeholder="2"
                  ></el-input-number>
                </el-form-item>
              </div>
              <div class="column2">
                 <!-- Keep -->
                 <el-form-item prop="keep">
                  <span slot="label">
                    KEEP
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.keepTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input v-model="db_form.keep" controls-position="right" class="form_item" style="width: 130px" placeholder="3650d"> </el-input>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    MAXROWS
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.maxRowsTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.maxrows"
                    :disabled="isEdit"
                    :min="1"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="4096"
                  ></el-input-number>
                </el-form-item>
                <el-form-item prop="retentions">
                  <span slot="label">
                    RETENTIONS
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.retentionsTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input size="small" v-model="db_form.retentions" :disabled="isEdit" controls-position="right" class="form_item" style="width: 130px"></el-input>
                </el-form-item>
              </div>
            </el-collapse-item>
            <el-collapse-item :title="$t('data.walParameters')" name="3">
              <div class="column1">    
                <el-form-item>
                  <span slot="label">
                    WAL_RETENTION_PERIOD
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.walRetentionPeriodTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.wal_retention_period"
                    :min="0"
                    :max="999999999999999"
                    :disabled="isEdit"
                    controls-position="right"
                    class="form_item"
                    placeholder="0s"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    WAL_ROLL_PERIOD
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.walRollPeriodTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.wal_roll_period"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="0s"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    WAL_RETENTION_SIZE
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.walRetentionSizeTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.wal_retention_size"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="0KB"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>   
              </div>
              <div class="column2">
                
                <el-form-item>
                  <span slot="label">
                    WAL_SEGMENT_SIZE
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.walSegmentSizeTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.wal_segment_size"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    class="form_item"
                    placeholder="0KB"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    WAL_LEVEL
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.walLevelTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.wal_level"
                    :min="1"
                    :max="2"
                    controls-position="right"
                    class="form_item"
                    placeholder="1"
                  ></el-input-number>
                </el-form-item>
                <el-form-item v-if="db_form.wal_level == 2">
                  <span slot="label">
                    WAL_FSYNC_PERIOD
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.walFsyncPeriodTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.wal_fsync_period"
                    :min="0"
                    :max="180000"
                    controls-position="right"
                    class="form_item"
                    placeholder="3000ms"
                  ></el-input-number>
                  <!-- <span class="inputUnit">ms</span> -->
                </el-form-item>     
              </div>
            </el-collapse-item>
            <el-collapse-item :title="$t('data.specialParameters')" name="4">
              <div class="column1">
                <el-form-item>
                  <span slot="label"
                    >SINGLE_STABLE
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.singleStableTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon> </el-tooltip
                  ></span>
                  <el-switch
                    size="small"
                    v-model="db_form.single_stable"
                    :disabled="isEdit"
                    :inactive-value="0"
                    :active-value="1"
                    controls-position="right"
                    class="form_item"
                  ></el-switch>
                </el-form-item>
                <!-- Precision -->
                <el-form-item>
                  <span slot="label">
                    PRECISION
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.precisionTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-select size="small" placeholder="ms" :disabled="isEdit" v-model="db_form.precision" style="width: 130px">
                    <el-option value="ms"></el-option>
                    <el-option value="us"></el-option>
                    <el-option value="ns"></el-option>
                  </el-select>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    REPLICA
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.replicaTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-select v-model="db_form.replica" placeholder="1" class="w100" style="width: 130px">
                    <el-option :value=1></el-option>
                    <el-option :value=3></el-option>
                  </el-select>
                </el-form-item>  
              </div>
              <div class="column2">
                <el-form-item>
                  <span slot="label">
                    TABLE_PREFIX
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.tablePrefixTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.table_prefix"
                    :min="-191"
                    :max="191"
                    controls-position="right"
                    class="form_item"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <span slot="label">
                    TABLE_SUFFIX
                    <el-tooltip placement="bottom" effect="light">
                      <div slot="content" v-html="$t('data.tableSuffixTip')"></div>
                      <Icon name="info" class="lableTips_icon"></Icon>
                    </el-tooltip>
                  </span>
                  <el-input-number
                    size="small"
                    v-model="db_form.table_suffix"
                    :min="-191"
                    :max="191"
                    controls-position="right"
                    class="form_item"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>                 
              </div>
            </el-collapse-item>
          </el-collapse>
        </el-form>
        <div class="confirm_line" size="medium">
          <el-button size="small" :disabled="requestIng" :loading="requestIng" type="primary" @click="handleCreateDb">{{
            $t(!isEdit ? "create" : "change")
          }}</el-button>
          <el-button size="small" :disabled="requestIng" @click="cancel">
            {{ $t("cancel") }}
          </el-button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
  import Icon from "@/components/Icon/index";
  import { validDatabaseName, validUnit, validRetentions } from "@/utils/validate";
  export default {
    data() {
      return {
        requestIng: false,
        activeNames: ['1'],
      };
    },
    components: { Icon },
    computed: {
      db_form() {
        return this.$store.state.dbs.db_form;
      },
      isEdit() {
        return this.$store.state.dbs.formStatus == "update";
      },
      formTitle() {
        return this.isEdit ? this.$t("data.editDatabase") : this.$t("data.createDatabase");
      },
      rules() {
        return {
          name: [
            {
              required: true,
              message: this.$t("data.nameTip"),
              trigger: "blur",
            },
            {
              validator: (_, value, callback) => {
                callback(validDatabaseName(value) ? undefined : new Error(this.$t("data.nameTip")));
              },
              trigger: "blur",
            },
          ],
          keep: [
            { 
              validator: this.checkKeep,
              trigger: "blur",  
            }
          ],
          retentions: [
            { 
              validator: this.checkRetentions,
              trigger: "blur",  
            }
          ]
        };
      },
      keepMin() {
        return Number(this.db_form.days) > 30 ? Number(this.db_form.days) : 30;
      },
    },
    methods: {
      handleCreateDb() {
        if (this.requestIng) return;
        this.$refs["dbForm1"].validate(valid => {
          if (valid) {
            this.requestIng = true;
            this.$store
            .dispatch("dbs/createDatabase", true)
            .then(() => {
              this.isEdit
                ? this.$message({
                  type: "success",
                  message: this.$t("changeSucc"),
                })
                : this.$message({
                  type: "success",
                  message: this.$t("createSucc"),
                });
            })
            .catch((err) => {
              this.$message({
                type: "error",
                message: err?.desc
              })
            })
            .finally(() => (this.requestIng = false));
          }
        });
      },
      cancel() {
        this.$store.commit("console/CANCEL_DETAIL");
      },
      checkKeep(_, value, callback) {
        if (!validUnit(value)) {
          return callback(new Error(this.$t('formatWrong')));
        } else {
          callback()
        }
      },
      checkRetentions(_, value, callback) {
        if (!validRetentions(value)) {
          return callback(new Error(this.$t('formatWrong')));
        } else {
          callback()
        }
      }
    },
  };
</script>

<style scoped>
  .dbCreate {
    overflow-x: auto;
  }

  .dbCreate_title {
    font-size: 24px;
    font-weight: 400;
  }

  .formWrapper {
    padding-right: 18px;
  }

  .sub_title {
    width: 100%;
    font-size: 24px;
    font-weight: 400;
    border-bottom: 1px solid #dfdfdf;
    margin-top: 20px;
    padding-bottom: 15px;
  }

  .section2 {
    min-width: 700px;
    overflow-x: auto;
    padding-bottom: 50px;
  }

  .form_style1 {
    margin-top: 20px;
    width: 680px;
  }

  .form_style_2col {
    margin-top: 30px;
  }
  ::v-deep .el-collapse-item__content {
    display: flex;
    flex-direction: row;
    justify-content: space-evenly;
  }
  ::v-deep .el-collapse-item__header {
    font-size: 16px;
  }

  .column2, .column3 {
    margin-left: 50px;
  }

  .lableTips_icon {
    width: 16px;
    height: 16px;
    color: #bfbfbf;
    top: 3px;
    position: relative;
    cursor: pointer;
  }

  .inputUnit {
    margin-left: 15px;
    font-size: 16px;
  }

  .confirm_line {
    margin-top: 30px;
  }
</style>
