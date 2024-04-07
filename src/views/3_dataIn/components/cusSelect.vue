<template>
  <div class="custom-select">
    <div
      @click="showOption"
      class="custom-input"
    >
      <el-input
        autocomplete="off"
        :readonly="true"
        placeholder="请选择，格式 key1,key2,key3=key3_alias"
        size="small"
        v-model="expression"
      >
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
    }
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
        this.expression = result?.join(',')
        this.$emit('updateData', this.expression)
      }
    }
  },
  methods: {
    showOption() {
      this.isShow = !this.isShow
      if (this.isShow) {
        this.selectJson()
      }
    }
  }
};
</script>

<style scoped>
.custom-select {
  position: relative;
  cursor: pointer;
}
.custom-input {
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
