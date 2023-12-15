<template>
  <div class="">
    <el-table
      size="mini"
      style="margin-bottom: 16px"
      :data="data"
    >
      <el-table-column
        align="center"
        width="100"
        :label="$t('datasource.primarykey')"
      >
        <template slot-scope="{ row }">
          <el-checkbox
            :value="isPrimaryKey(row)"
            :disabled="isCanBePrimaryKey(row)"
            @change="primaryKeyChange(row)"
            size="mini"
          ></el-checkbox>
        </template>
      </el-table-column>
      <el-table-column
        align="center"
        :label="$t('datasource.usageType')"
      >
        <template slot-scope="{ row }">
          <el-select
            v-model="row.usageType"
            @change="usageTypeChange($event, row)"
            :disabled="!row.field || isPrimaryKey(row)"
            size="mini"
          >
            <el-option
              v-for="item in usageTypeList"
              :key="item.value"
              v-bind="item"
            ></el-option>
          </el-select>
        </template>
      </el-table-column>
      <el-table-column
        prop="field"
        align="center"
        :label="$t('datasource.rename')"
      >
        <template slot-scope="{ row }">
          <span v-if="row.type == 'system'">{{ row.field }}</span>
          <el-input
            v-else
            v-model="row.field"
            size="mini"
          ></el-input>
        </template>
      </el-table-column>
      <!-- <el-table-column
        align="center"
        :label="$t('datasource.rename')"
      >
        <template slot-scope="{ row }">
          <el-icon
            v-if="row.type == 'system'"
            class="el-icon-close"
          ></el-icon>
          <el-input
            v-else
            v-model="row.targetField"
            size="mini"
          ></el-input>
        </template>
      </el-table-column>
      <el-table-column
        align="center"
        width="200"
        :label="$t('dataIn.dataType')"
      >
        <template slot-scope="{ row }">
          <el-icon
            v-if="row.type == 'system'"
            class="el-icon-close"
          ></el-icon>
          <DataTypeSelect
            v-else
            v-model="row.dataType"
            @change="handleDataType($event, row)"
          />
        </template>
      </el-table-column> -->
      <el-table-column
        align="center"
        fixed="right"
        width="52"
      >
        <template slot-scope="{ row }">
          <el-button
            v-if="row.type != 'system'"
            icon="el-icon-delete"
            @click="del(row)"
            class="mini-btn"
            plain
          ></el-button>
        </template>
      </el-table-column>
      <!-- <template slot="append">
        <el-button
          icon="el-icon-plus"
          size="mini"
          style="margin-top: 1px"
          type="primary"
          class="w100"
          @click="add"
        ></el-button>
      </template> -->
    </el-table>
  </div>
</template>

<script>
import DataTypeSelect from './dataTypeSelect.vue';
export default {
  props: {
    data: {
      type: Array,
      default: () => []
    },
    value: {
      type: String,
      default: ''
    }
  },
  inject: ['sourceParent'],
  components: { DataTypeSelect },
  data() {
    return {
      usageTypeList: [
        {
          label: 'None',
          value: 0
        },
        {
          label: this.$t('stream.column'),
          value: 1
        },
        {
          label: this.$t('alert.tag'),
          value: 2
        }
      ]
    };
  },
  computed: {
    isEdit() {
      return this.sourceParent.isEditable;
    }
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {
    add() {
      this.data.push({
        usageType: 1,
        field: '',
        targetField: '',
        dataType: 'INT',
        type: 'custome'
      });
    },
    primaryKeyChange(row) {
      this.$emit('input', row.field);
    },
    isCanBePrimaryKey(row) {
      return this.isEdit || (row.field != 'DateTime');
    },
    isPrimaryKey(row) {
      return row.field == this.value;
    },
    del(data) {
      this.data.splice(this.data.indexOf(data), 1);
      if (this.value == data.field) {
        this.$emit('input', 'ts');
      }
    },
    usageTypeChange(type, row) {
      // 监听 table 的变化，修改 type 的 disabled
      console.log('sourceParent',this.sourceParent.sourceForm.data);
      if (type != 1 && this.value == row.field) {
        this.$emit('input', '');
      }
    },
    handleDataType(val, row) {
      if (val != 'TIMESTAMP' && this.value == row.field) {
        this.$emit('input', '');
      }
    }
  }
};
</script>

<style scoped lang="scss"></style>
