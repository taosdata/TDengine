<template>
  <div class="cascader">
    <ul v-if="!cluster" class="cascader-content">
      <el-empty v-if="!clusterList.length" :image-size="48"></el-empty>
      <li
        v-for="(item, index) in clusterList"
        :title="item.label"
        @click="selectCluster(index)"
        :class="{ 'is-active': index == currentCluster }"
        :key="item.value"
      >
        <el-checkbox class="check-class" @click.stop v-model="item.checked"></el-checkbox>

        {{ item.label }}

        <!-- <section class="el-cascader-node__postfix">
          <el-popover :disabled="!item.checked" placement="right" width="245" trigger="click">
            <el-date-picker size="mini" v-model="item.expire" type="datetime" :placeholder="$t('expire')"> </el-date-picker>

            <el-icon :class="{ 'btn-disabled': !item.checked }" slot="reference" class="el-icon-date"></el-icon>
          </el-popover>
        </section> -->
      </li>
    </ul>
    <ul v-if="currentCluster != -1" :key="currentClusterData.value" class="cascader-content">
      <li>
        <el-checkbox
          @change="selectAll"
          :disabled="!currentClusterData.checked || currentClusterData.disabledSelectAll"
          v-model="currentClusterData.selectAll"
        >
          {{ $t("users.selectAll") }}
        </el-checkbox>
      </li>
      <el-empty v-if="!dbList.length" :description="$t('data.noDatabase')" :image-size="48"></el-empty>
      <li
        v-for="item in dbList"
        :class="{ 'is-active': item.name == currentDBData.name }"
        :key="item.name + currentClusterData.value"
        @click="selectDB(item)"
        style="padding-left: 45px"
        :title="item.name"
      >
        <el-checkbox
          @click.stop
          :disabled="!currentClusterData.checked || currentClusterData.selectAll"
          class="check-class"
          v-model="item.checked"
          @change="changeDB"
        ></el-checkbox>
        {{ item.name }}
      </li>
    </ul>

    <ul v-if="currentDBData.name" class="cascader-content">
      <li>
        <el-checkbox :disabled="!currentDBData.checked || currentClusterData.selectAll" v-model="currentDBData.read">
          {{ $t("users.read") }}
        </el-checkbox>
      </li>

      <li>
        <el-checkbox :disabled="!currentDBData.checked || currentClusterData.selectAll" v-model="currentDBData.write">
          {{ $t("users.write") }}
        </el-checkbox>
      </li>
    </ul>
  </div>
</template>

<script>
  import { getDBListReq } from "@/api/gateway/data/dbs";
  export default {
    props: {
      filterList: {
        type: Array,
        default: function () {
          return [];
        },
      },
      cluster: {
        type: Object,
      },
    },
    data() {
      return {
        currentCluster: -1,
        loading: false,
        dbList: [],
        clusterList: [],
        clusterHistory: {},
        // 用于过滤已选中的数据库
        dbFilterList: [],
        currentDBData: {},
      };
    },
    computed: {
      currentClusterData() {
        return this.clusterList[this.currentCluster] || {};
      },
    },
    watch: {
      filterList: {
        handler() {
          this.clusterList = this.$store.state.app.clusters
            .filter(item => item.cluster_status == "Running" && !this.filterList.includes(item.id))
            .map(item => ({ label: item.alias || item.name, value: item.id, checked: false, selectAll: false, expire: "" }));
          this.currentCluster = -1;
        },
        immediate: true,
      },
      // 当设置集群id时，选择集群列表隐藏
      cluster: {
        handler(newval) {
          if (newval) {
            let index = this.clusterList.findIndex(item => item.value == newval.value);
            this.clusterList[index] = newval;
            this.dbFilterList = newval.dbList.map(item => item.name);
            this.selectCluster(index, true);
          } else {
            this.selectCluster(-1);
            this.dbFilterList = [];
          }
        },
        immediate: true,
      },
    },
    methods: {
      selectCluster(index, update = false) {
        if (this.currentCluster == index && !update) return;
        this.currentCluster = index;
        this.dbList = [];
        this.currentDBData = {};
        this.loadDB();
      },
      selectDB(data) {
        this.currentDBData = data;
      },
      selectAll(val) {
        if (val) {
          this.dbList.forEach(item => {
            item.checked = true;
            item.read = true;
            item.write = true;
          });
        } else {
          this.dbList.forEach(item => {
            item.checked = false;
            item.read = false;
            item.write = false;
          });
        }
        this.currentDBData = this.dbList[0];
      },
      changeDB(val) {
        if (val) {
          this.currentDBData.read = true;
        } else {
          this.currentDBData.read = false;
          this.currentDBData.write = false;
        }
      },
      async loadDB() {
        this.loading = true;
        let clusterKey = this.currentClusterData.value;
        // 判断是不是拥有历史记录
        if (this.clusterHistory[clusterKey]) {
          let list = this.clusterHistory[clusterKey].filter(item => !this.dbFilterList.includes(item.name));
          this.dbList = list;
          this.loading = false;
          return;
        }
        await getDBListReq(this.currentClusterData.value)
          .then(data => {
            let selectAll = this.currentClusterData.selectAll;
            data = data.map(item => {
              return { name: item.name, read: selectAll, write: selectAll, checked: selectAll };
            });
            this.dbList = data.filter(item => !this.dbFilterList.includes(item.name));
            if (!this.clusterHistory[clusterKey]) {
              this.clusterHistory[clusterKey] = data;
            }
          })
          .catch(() => {});
        this.loading = false;
      },
      getValue() {
        let result = [];
        this.clusterList.forEach(item => {
          if (item.checked) {
            this.clusterHistory[item.value].forEach(db => {
              if (db.checked && (db.read || db.write)) {
                result.push(item.value + ":" + db.name + ":" + (db.read ? "r" : "") + (db.write ? "w" : ""));
              }
            });
          }
        });
        this.$emit("validate", result);
        return result;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .cascader {
    // width: 100%;
    display: flex;
    border: 1px solid #e4e7ed;
    border-radius: 4px;
    .cascader-content {
      flex: 1;
      min-width: 200px;
      font-size: 14px;
      height: 204px;
      overflow: auto;
      padding: 6px 0;
      border-radius: 4px;
      border-right: 1px solid #e4e7ed;
      li {
        padding: 0 30px 0 20px;
        position: relative;
        line-height: 20px;
        outline: none;
        cursor: pointer;
        @extend .nowrap;
        display: flex;
        align-items: center;
        border-radius: 5px;
      }
      .check-class {
        margin-right: 10px;
      }
      .is-active {
        background: $color-primary;
        color: #fff;
      }
      .el-cascader-node__postfix {
        position: absolute;
        right: 10px;
      }
    }
  }
</style>
