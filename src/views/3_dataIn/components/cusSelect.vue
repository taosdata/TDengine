<template>
  <div class="custom-select" 
    ref="myDiv" 
    @click="divClicked">
    <template 
      v-if="String(depth) !== 'undefined'"
    >
      <div
        @click="showOption"
        class="custom-input"
      >
        <el-input
          autocomplete="off"
          :readonly="true"
          :placeholder="$t('datasource.transformer.jsonPlaceholder')"
          size="small"
          v-model="expression"
        >
          <i slot="suffix" :class="['el-input__icon', isShow ? 'el-icon-arrow-up' : 'el-icon-arrow-down']"></i>
        </el-input>
      </div>
      <ul class="custom-ul" v-if="isShow">
        <li v-for="proper in allProperties" :key="proper.defaultValue" class="custom-li">
          <el-checkbox class="my-checkbox" v-model="proper.checked">
            <span style="width: 200px;">{{ proper.defaultValue }}</span>
          </el-checkbox>
          <el-input style="margin-left: 4px; width: 200px" size="mini" :key="proper.defaultValue" v-model="proper.rename"></el-input>
        </li>
      </ul>
    </template>
     <el-input
      v-else
      v-model="expression"
      @blur="$emit('updateData', expression)"
      :placeholder="$t('datasource.transformer.jsonPlaceholder')"
      size="small"
    >
    </el-input>
  </div>
</template>

<script>
export default {
  data() {
   return {
    isShow: false,
    expression: ''
   }
  },
  props: {
    allProperties: {
      type: Array,
      default: () => {
        return [];
      }
    },
    selectJson: {
      type: Function,
    },
    depth: {
      type: Number
    },
    value: {
      type: String,
      default: ''
    },
  },
  watch: {
    allProperties: {
      deep: true,
      immediate: true,
      handler(newVal){
        let result = []
        newVal.map(item => {
          if (item.checked) {
            item.rename ? result.push(`${item.defaultValue}=${item.rename}`) : result.push(item.defaultValue)
          }
        })
        if (String(this.depth) !== 'undefined') {
          this.expression = result?.join(',')
        } else [
          this.expression = this.value
        ]
        this.$emit('updateData', this.expression)
      }
    }
  },
  mounted() {
    // 在mounted钩子中添加事件监听
    document.addEventListener('click', this.documentClicked);
  },
  beforeDestroy() {
    // 在组件销毁前移除事件监听
    document.removeEventListener('click', this.documentClicked);
  },
  methods: {
    showOption() {
      this.isShow = !this.isShow
      if (this.isShow) {
        this.selectJson()
      }
    },
    divClicked() {
      // 阻止冒泡
      event.stopPropagation();
    },
    documentClicked(event) {
      // 如果点击的是div外部，执行外部点击的操作
      if (!this.$refs.myDiv.contains(event.target)) {
        this.isShow = false;
      }
    }
  }
};
</script>

<style scoped>
.custom-select {
  position: relative;
  display: inline-block;
  width: 100%;
}
.custom-input 
::v-deep .el-input__inner:hover {
  cursor: pointer;
}
.custom-ul {
  padding: 10px;
  position: absolute;
  background: white;
  max-height: 300px;
  overflow: auto;
  width: 100%;
  z-index: 100;
  border: 1px solid #eee;
  border-radius: 4px;
}

.custom-li{
  margin-bottom: 5px;
  display: flex;
  justify-content: space-between;
}

.my-checkbox {
  display: block;
}

</style>
