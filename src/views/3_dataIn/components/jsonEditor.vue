<template>
  <div style="width: 400px; height: 400px"></div>
</template>

<script>
import * as JSONEditor from "jsoneditor";
import "jsoneditor/dist/jsoneditor.css";

export default {
  name: "JsonEditor",
  model: {
    prop: "value",
    event: "change",
  },
  props: {
    value: {
      type: [String, Object, Array],
      default: () => {
        return [
          {
            timestamp: 1699324881000,
            groupid: "5",
            location: "Beijing",
            deviceid: 3750,
            current: 7.5,
            voltage: 181,
            phase: 0.1539,
          },
          {
            timestamp: 1699324882000,
            groupid: "6",
            location: "上海",
            deviceid: 3790,
            current: 9,
            voltage: 200,
            phase: 0.1902,
          },
        ];
      },
    },
  },
  data() {
    return {
      editor: null,
      isStrValue: false,
    };
  },
  mounted() {
    let initJson = "";
    if (this.value) {
      if (typeof this.value === "string") {
        this.isStrValue = true;
        initJson = JSON.parse(this.value);
      } else {
        initJson = this.value;
      }
      this.initEditor(initJson);
    }
  },
  methods: {
    initEditor(initJson) {
      this.editor = new JSONEditor(
        this.$el,
        {
          mode: "code",
          language: "zh-CN",
          mainMenuBar: false,
          search: true,
          onChange: () => {
            console.log('editor文本变化');
            if (this.isStrValue) {
              this.$emit("change", this.editor.getText());
            } else {
              this.$emit("change", this.editor.get());
            }
          },
          onBlur: () => {
            this.repair();
            this.format();
          },
        },
        initJson
      );
    },
    setJsonText(text) {
      this.editor.setText(text);
    },
    setJSON(obj) {
      this.editor.set(obj);
    },
    getJsonText() {
      return this.editor.getText();
    },
    getJSON() {
      return this.editor.get();
    },
    repair() {
      this.editor.repair();
    },
    format() {
      this.editor.format();
    },
    validate() {
      return this.editor.validate();
    },
    dispose() {
      this.editor.destroy();
    },
  },
};
</script>

<style scoped></style>
