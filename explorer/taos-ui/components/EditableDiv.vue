<template>
  <div
    :contenteditable="isEditing"
    class="editable-div"
    @dblclick="enableEditing"
    @blur="disableEditing"
    @input="updateValue"
  >
    {{ props.modelValue }}
  </div>
</template>

<script lang="ts" setup>
const props = defineProps<{
  modelValue: string;
}>();

const emits = defineEmits(['update:modelValue']);

const isEditing = ref(false);

const enableEditing = () => {
  isEditing.value = true;
};

const disableEditing = () => {
  isEditing.value = false;
};

const updateValue = (event: Event) => {
  const target = event.target as HTMLDivElement;
  emits('update:modelValue', target.innerText);
};
</script>

<style scoped>
.editable-div {
  min-height: 20px;
  padding: 8px;
  border: 1px solid #ccc;
}
</style>
