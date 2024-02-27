<template>
  <div ></div>
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
        return ''
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
    // if (this.value) {
      if (this.value&&typeof this.value === "string") {
        this.isStrValue = true;
        initJson = JSON.parse(this.value);
      } else {
        initJson = this.value;
      }
      this.initEditor(initJson);
    // }
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
            if (this.isStrValue) {
              this.$emit("change", this.editor.getText());
            } else {
                let iserror=false
                try {
                    this.editor.get()
                    iserror=true
                    this.$emit("change", this.editor.get());
                } catch (error) {
                    this.$emit("change", null);
                }
              
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
