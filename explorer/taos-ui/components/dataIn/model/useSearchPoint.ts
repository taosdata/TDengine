import { ref, watch, type ComponentInternalInstance } from 'vue';
import { validateFormFields, isShowDatasetTable, datasetTableData, formatFromData } from './util';
import { getDataInProps } from './useDataIn';

// 主要是获取预览 opc 点位数据
// 1. 校验 form 表单
// 2. 获取Ticket
// 2. 检查数据是否准备完成checkReadyFile
// 4. 最后根据 complete 将数据展示在右边

export default function () {
  const dataInProps = getDataInProps();
  const sourceParent = inject<ComponentInternalInstance>('sourceParent') as any;
  const loading = ref(false);
  const isComplete = ref(false);
  const ticket = ref('');
  const categoryOpc = ref('PointList');
  const timer = ref();

  watch(isComplete, async val => {
    console.log('再hook组件中监听isComplete=', isComplete);
    if (val) {
      if (timer.value) {
        clearInterval(timer.value);
      }
      getDatasetsData();
      loading.value = false;
    }
  });

  function onValid(param: any, agent: number) {
    // 使用统一的格式化方法，将前端表单结构转换为后端 from_json 结构
    const fromJson = formatFromData(param);
    readyData(fromJson, agent);
  }
  // Methods
  const search = () => {
    validateFormFields(sourceParent?.refs.formRef, onValid);
  };

  const readyData = async (from: Recordable, via: number | string) => {
    if (loading.value) return;
    try {
      loading.value = true;
      // 获取 ticket
      const params: Recordable = {
        from_json: from,
        categories: categoryOpc.value
      };

      if (via) {
        params.via = via;
      }
      const result = await dataInProps.dataSource.api.fechTicketApi(params);
      ticket.value = result.ticket;

      // 轮询查看数据是否准备完成
      timer.value = setInterval(async () => {
        const { complete } = await dataInProps.dataSource.api.checkReadyFile(result.ticket);
        isComplete.value = complete;
        isShowDatasetTable.value = complete;
      }, 2000);
    } catch (error) {
      if (timer.value) clearInterval(timer.value);
    }
  };

  async function getDatasetsData() {
    const res = await dataInProps.dataSource.api.getDatasets(ticket.value, 1, 1000000);
    datasetTableData.value = res;
    isComplete.value = false;
    loading.value = false;
  }

  return {
    loading,
    timer,
    isComplete,
    ticket,
    search
  };
}
