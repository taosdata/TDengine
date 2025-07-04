<template>
  <div class="pb-20px">
    <h3 v-if="showTitle" class="form-title">{{ formTitle }}</h3>
    <el-form ref="formIns" class="mt-20px" label-position="left" label-width="230px" :rules="rules" :model="formData">
      <div class="form-wrapper">
        <el-alert class="mb-20px!" type="warning" :title="t('db.backslashTip')"></el-alert>
        <el-form-item :label="t('common.name')" prop="name">
          <el-input v-model="formData.name" :disabled="isEdit" :maxlength="32" style="max-width: 620px" />
        </el-form-item>
        <el-form-item v-if="isHa" :label="t('db.replica')" prop="replica">
          <template #label>
            <span>
              REPLICA
              <el-tooltip placement="bottom" effect="light">
                <template #content>
                  <div v-dompurify-html="t('db.replicaTip', [isHa ? 3 : 1])"></div>
                </template>
                <Icon name="info" class="label-tips-icon"></Icon>
              </el-tooltip>
            </span>
          </template>
          <el-select v-model="formData.replica" :disabled="isRecplicaDisabled">
            <el-option v-for="item in replicaList" :key="item" :value="item"></el-option>
          </el-select>
          <p v-if="formData.replica == 1" class="errorText">{{ t('db.replica1Tip') }}</p>
        </el-form-item>
      </div>
      <div class="section2">
        <div class="sub-title">{{ t('common.configurationParameters') }}</div>
        <section class="form-content-col">
          <el-collapse v-model="activeNames" class="w-full">
            <el-collapse-item :title="t('db.performanceRelatedParameters')" name="1">
              <div class="column1">
                <!-- BUFFER -->
                <el-form-item>
                  <template #label>
                    <span>
                      BUFFER
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.bufferTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.buffer"
                    :min="3"
                    :max="16384"
                    controls-position="right"
                    placeholder="96MB"
                  >
                  </el-input-number>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('tsdb_pagesize')">
                  <template #label>
                    <span>
                      TSDB_PAGESIZE
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.tsdbPagesizeTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.tsdb_pagesize"
                    :min="1"
                    :max="16384"
                    controls-position="right"
                    placeholder="4KB"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>
                <!-- CACHEMODEL -->
                <el-form-item>
                  <template #label>
                    <span>
                      CACHEMODEL
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.cacheModelTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-select v-model="formData.cachemodel" placeholder="none">
                    <el-option value="none"></el-option>
                    <el-option value="last_row"></el-option>
                    <el-option value="last_value"></el-option>
                    <el-option value="both"></el-option>
                  </el-select>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('strict')">
                  <template #label>
                    <span
                      >STRICT
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.strictTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon> </el-tooltip
                    ></span>
                  </template>

                  <el-switch
                    v-model="formData.strict"
                    :disabled="isEdit"
                    inactive-value="off"
                    active-value="on"
                    controls-position="right"
                  ></el-switch>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      PAGESIZE
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.pageSizeTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.pagesize"
                    :disabled="isEdit"
                    :min="1"
                    :max="16384"
                    controls-position="right"
                    placeholder="4kb"
                  ></el-input-number>
                </el-form-item>
              </div>
              <div class="column2">
                <el-form-item>
                  <template #label>
                    <span>
                      PAGES
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.pagesTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.pages"
                    :min="64"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="256"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      VGROUPS
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.vgroupsTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.vgroups"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="2"
                  ></el-input-number>
                </el-form-item>

                <el-form-item v-if="dbParamsterList.includes('stt_trigger')">
                  <template #label>
                    <span>
                      STT_TRIGGER
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.sttTaiggerTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.stt_trigger"
                    :min="1"
                    :max="16"
                    controls-position="right"
                    placeholder="1"
                  ></el-input-number>
                </el-form-item>
                <!-- CACHESIZE -->
                <el-form-item>
                  <template #label>
                    <span>
                      CACHESIZE
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.cacheSizeTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.cachesize"
                    :min="1"
                    :max="65536"
                    controls-position="right"
                    placeholder="1MB"
                  >
                  </el-input-number>
                </el-form-item>
              </div>
            </el-collapse-item>
            <el-collapse-item :title="t('db.dataPersistenceParameters')" name="2">
              <div class="column1">
                <el-form-item>
                  <template #label>
                    <span>
                      DURATION
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.durationTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input
                    v-model="formData.duration"
                    :disabled="isEdit"
                    controls-position="right"
                    placeholder="50d"
                  ></el-input>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      MINROWS
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.minRowsTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.minrows"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="100"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      COMP
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.compTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.comp"
                    :disabled="isEdit"
                    :min="0"
                    :max="2"
                    controls-position="right"
                    placeholder="2"
                  ></el-input-number>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('s3_keeplocal')">
                  <template #label>
                    <span>
                      S3 KEEPLOCAL
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.s3KeepLocalTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input v-model="formData.s3_keeplocal" :disabled="isEdit" controls-position="right"></el-input>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('s3_chunkpages')">
                  <template #label>
                    <span>
                      S3 CHUNKPAGES
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.s3ChunkPagesTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.s3_chunkpages"
                    :disabled="isEdit"
                    :min="131072"
                    :max="1048576"
                    controls-position="right"
                  ></el-input-number>
                </el-form-item>
              </div>
              <div class="column2">
                <!-- Keep -->
                <el-form-item prop="keep">
                  <template #label>
                    <span>
                      KEEP
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.keepTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input v-model="formData.keep" controls-position="right" placeholder="3650d"> </el-input>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      MAXROWS
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.maxRowsTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.maxrows"
                    :disabled="isEdit"
                    :min="1"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="4096"
                  ></el-input-number>
                </el-form-item>
                <el-form-item prop="retentions">
                  <template #label>
                    <span>
                      RETENTIONS
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.retentionsTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input v-model="formData.retentions" :disabled="isEdit" controls-position="right"></el-input>
                </el-form-item>

                <el-form-item v-if="dbParamsterList.includes('s3_compact')">
                  <template #label>
                    <span>
                      S3 COMPACT
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.s3CompactTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-select v-model="formData.s3_compact" :disabled="isEdit">
                    <el-option value="0"></el-option>
                    <el-option value="1"></el-option>
                  </el-select>
                </el-form-item>
              </div>
            </el-collapse-item>
            <el-collapse-item :title="t('db.walParameters')" name="3">
              <div class="column1">
                <el-form-item>
                  <template #label>
                    <span>
                      WAL_RETENTION_PERIOD
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.walRetentionPeriodTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.wal_retention_period"
                    :min="3600"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="0s"
                  ></el-input-number>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('wal_roll_period')">
                  <template #label>
                    <span>
                      WAL_ROLL_PERIOD
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.walRollPeriodTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.wal_roll_period"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="0s"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      WAL_RETENTION_SIZE
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.walRetentionSizeTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.wal_retention_size"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="0KB"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>
              </div>
              <div class="column2">
                <el-form-item v-if="dbParamsterList.includes('wal_segment_size')">
                  <template #label>
                    <span>
                      WAL_SEGMENT_SIZE
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.walSegmentSizeTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.wal_segment_size"
                    :disabled="isEdit"
                    :min="0"
                    :max="999999999999999"
                    controls-position="right"
                    placeholder="0KB"
                  ></el-input-number>
                </el-form-item>
                <el-form-item>
                  <template #label>
                    <span>
                      WAL_LEVEL
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.walLevelTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.wal_level"
                    :min="1"
                    :max="2"
                    controls-position="right"
                    placeholder="1"
                  ></el-input-number>
                </el-form-item>
                <el-form-item v-if="formData.wal_level == 2">
                  <template #label>
                    <span>
                      WAL_FSYNC_PERIOD
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.walFsyncPeriodTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.wal_fsync_period"
                    :min="0"
                    :max="180000"
                    controls-position="right"
                    placeholder="3000ms"
                  ></el-input-number>
                </el-form-item>
              </div>
            </el-collapse-item>
            <el-collapse-item :title="t('db.specialParameters')" name="4">
              <div class="column1">
                <el-form-item>
                  <template #label>
                    <span
                      >SINGLE_STABLE
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.singleStableTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon> </el-tooltip
                    ></span>
                  </template>
                  <el-switch
                    v-model="formData.single_stable"
                    :disabled="isEdit"
                    :inactive-value="0"
                    :active-value="1"
                    controls-position="right"
                  ></el-switch>
                </el-form-item>
                <!-- Precision -->
                <el-form-item>
                  <template #label>
                    <span>
                      PRECISION
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.precisionTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-select v-model="formData.precision" placeholder="ms" :disabled="isEdit">
                    <el-option value="ms"></el-option>
                    <el-option value="us"></el-option>
                    <el-option value="ns"></el-option>
                  </el-select>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('ENCRYPT_ALGORITHM')">
                  <template #label>
                    <span>
                      ENCRYPT
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.encryptTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-select v-model="formData.ENCRYPT_ALGORITHM" :disabled="isEdit">
                    <el-option value="none"></el-option>
                    <el-option value="sm4"></el-option>
                  </el-select>
                </el-form-item>
              </div>
              <div class="column2">
                <el-form-item v-if="dbParamsterList.includes('table_prefix')">
                  <template #label>
                    <span>
                      TABLE_PREFIX
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.tablePrefixTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.table_prefix"
                    :min="-191"
                    :max="191"
                    controls-position="right"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>
                <el-form-item v-if="dbParamsterList.includes('table_suffix')">
                  <template #label>
                    <span>
                      TABLE_SUFFIX
                      <el-tooltip placement="bottom" effect="light">
                        <template #content>
                          <div v-dompurify-html="t('db.tableSuffixTip')"></div>
                        </template>
                        <Icon name="info" class="label-tips-icon"></Icon>
                      </el-tooltip>
                    </span>
                  </template>
                  <el-input-number
                    v-model="formData.table_suffix"
                    :min="-191"
                    :max="191"
                    controls-position="right"
                    :disabled="isEdit"
                  ></el-input-number>
                </el-form-item>
              </div>
            </el-collapse-item>
          </el-collapse>
        </section>
      </div>
    </el-form>
    <section class="flex-center mt-20px" label-width="0">
      <el-button :disabled="requesting" :loading="requesting" type="primary" @click="handleCreateDb">{{
        t('common.' + (isEdit ? 'update' : 'create'))
      }}</el-button>
      <el-button :disabled="requesting" @click="cancle">
        {{ t('common.cancel') }}
      </el-button>
    </section>
  </div>
</template>

<script lang="ts" setup>
import { validDbDuration, validDbKeep, validTDKeywords } from 'utils/validate';
import { CreateDbProps } from './props';
import { getDbParamsByTdVersion, rmStrBackquote } from 'utils/tdengine';
import { FormInstance, ElMessage } from 'element-plus';
import { t } from 'locales';

const props = withDefaults(defineProps<CreateDbProps>(), {
  showTitle: true,
  dbList: () => [],
  isHa: false,
  isEdit: false
});
const dbParameters: Recordable = getDbParamsByTdVersion(props.version);
const dbParamsterList = Object.keys(dbParameters);
const formData = ref({ ...(props.formData ?? dbParameters) });
const replicaList = [1, 3];
const requesting = ref(false);
const activeNames = ref(['1']);
const formIns = shallowRef<FormInstance | null>(null);
const formTitle = props.isEdit ? t('db.edit') : t('db.create');
const rules = {
  name: [
    {
      required: true,
      message: t('common.requiredTemp', [t('common.name')]),
      trigger: 'blur'
    },
    {
      validator: (_: any, value: string, callback: AnyFunction) => {
        const dbName = rmStrBackquote(value);
        if (validTDKeywords(dbName)) {
          callback(new Error(t('explorer.tdKewordTip', [dbName])));
        } else if (!props.isEdit && props.dbList.some(item => item.name == dbName)) {
          callback(new Error(t('db.nameExisted', [dbName])));
        } else {
          callback();
        }
      },
      trigger: 'blur'
    }
  ],
  duration: [
    {
      required: true,
      message: t('common.requiredTemp', ['DURATION']),
      trigger: 'blur'
    },
    {
      validator: (_: any, value: string, callback: AnyFunction) => {
        callback(validDbDuration(value) ? undefined : new Error(t('common.formatErrorTemp', ['DURATION'])));
      },
      trigger: 'blur'
    }
  ],
  keep: [
    {
      required: true,
      message: t('common.requiredTemp', ['KEEP']),
      trigger: 'blur'
    },
    {
      validator: (_: any, value: string, callback: AnyFunction) => {
        callback(validDbKeep(value) ? undefined : new Error(t('common.formatErrorTemp', ['KEEP'])));
      },
      trigger: 'blur'
    }
  ]
};
const isRecplicaDisabled = computed(() => formData.value.replica == 3 && props.isEdit);
const emits = defineEmits(['cancel', 'success', 'update']);

function handleCreateDb() {
  if (requesting.value || !formIns.value) return;
  formIns.value.validate().then(() => {
    const data = props.isEdit ? {} : formData.value;
    if (props.isEdit) {
      for (const key in props.formData) {
        if (props.formData[key] != formData.value[key]) {
          data[key] = formData.value[key];
        }
      }
      if (Object.keys(data).length == 0) {
        return cancle();
      }
      data.name = formData.value.name;
    }
    requesting.value = true;
    props
      .updateApi(data)
      .then(() => {
        ElMessage.success(t('msg.' + (props.isEdit ? 'update' : 'create') + 'Success'));
        emits('update', data.name);
        emits('success');
      })
      .finally(() => {
        requesting.value = false;
      });
  });
}
function cancle() {
  emits('cancel');
}
</script>

<style lang="scss" scoped>
.form-wrapper {
  padding-right: 18px;
}

.form-title {
  font-size: 24px;
  font-weight: 400;
}

.sub-title {
  width: 100%;
  margin-top: 20px;
  font-size: 24px;
  font-weight: 400;
}

.section2 {
  min-width: 700px;
  overflow-x: auto;

  &:deep(.el-select) {
    width: 150px;
    min-width: 150px;
  }

  &:deep(.el-input) {
    width: 150px;
  }
}

.form-content-col {
  display: flex;
  flex-direction: row;
  margin-top: 30px;
  overflow: hidden;

  &:deep(.el-collapse-item__content) {
    display: flex;
    flex-direction: row;
    justify-content: space-evenly;
  }

  &:deep(.el-collapse-item__header) {
    font-size: 18px;
  }
}

.column1 {
  min-width: 400px;
}

.column2 {
  width: 400px;
  margin-left: 50px;
}

.label-tips-icon {
  position: relative;
  top: 3px;
  width: 16px;
  height: 16px;
  color: #bfbfbf;
  cursor: pointer;
}
</style>
