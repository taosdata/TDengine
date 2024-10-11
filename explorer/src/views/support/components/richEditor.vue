<template>
  <div>
    <quill-editor
      :content="value"
      :options="editorOption"
      @change="onEditorChange($event)"
    />
  </div>
</template>

<script>
import "quill/dist/quill.core.css";
import "quill/dist/quill.snow.css";
import "quill/dist/quill.bubble.css";
import { quillEditor } from "vue-quill-editor";

export default {
  props: {
    value: {
      type: [Number, Object, Array, String],
      default: ""
    }
  },
  components: {
    quillEditor
  },
  data() {
    return {};
  },
  computed: {
    editorOption() {
      return {
        modules: {
          toolbar: {
            container: [
              ["bold", "italic", "underline", "strike"], // 加粗 斜体 下划线 删除线 -----['bold', 'italic', 'underline', 'strike']
              [{ color: [] }, { background: [] }], // 字体颜色、字体背景颜色-----[{ color: [] }, { background: [] }]
              [{ align: [] }], // 对齐方式-----[{ align: [] }]

              [{ header: [1, 2, 3, 4, 5, 6, false] }], // 标题
              [{ direction: "ltl" }], // 文本方向-----[{'direction': 'rtl'}]
              [{ direction: "rtl" }], // 文本方向-----[{'direction': 'rtl'}]
              [{ indent: "-1" }, { indent: "+1" }], // 缩进-----[{ indent: '-1' }, { indent: '+1' }]
              [{ list: "ordered" }, { list: "bullet" }], // 有序、无序列表-----[{ list: 'ordered' }, { list: 'bullet' }]
              [{ script: "sub" }, { script: "super" }], // 上标/下标-----[{ script: 'sub' }, { script: 'super' }]
              ["blockquote", "code-block"], // 引用  代码块-----['blockquote', 'code-block']
              ["clean"] // 清除文本格式-----['clean']
            ]
          }
        },
        ...this.$attrs
      };
    }
  },
  methods: {
    onEditorChange({ html }) {
      this.$emit("input", html);
    }
  }
};
</script>

<style>
.ql-container {
  height: 300px;
}
</style>
