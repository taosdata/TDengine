<template>
  <Form v-bind="props" />
</template>

<script lang="ts" setup>
import Form from 'components/explorer/components/createSubTbForm.vue';

type Props = InstanceType<typeof Form>['$props'];

const props: Props = {
  dbData: {
    name: 'test'
  },
  isEdit: false,
  formData: {
    stbTmpl: 'stbTmpl',
    name: 'name',
    tags: [
      {
        field: 'tag1',
        value: '',
        type: 'INT',
        length: 8
      }
    ]
  },
  requestApi: (data: Recordable, dbName: string) =>
    new Promise((resolve: AnyFunction) => {
      console.log(data, dbName);
      resolve();
    })
};
</script>

<style scoped lang="scss"></style>
