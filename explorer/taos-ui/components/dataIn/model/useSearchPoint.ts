import { ref, type ComponentInternalInstance } from 'vue';
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

  const latestRequestId = ref(0);

  function clearPollingTimer() {
    if (timer.value) {
      clearInterval(timer.value);
      timer.value = undefined;
    }
  }

  function clearPreviewState() {
    clearPollingTimer();
    isComplete.value = false;
    isShowDatasetTable.value = false;
    datasetTableData.value = undefined;
    ticket.value = '';
  }

  function isLatestRequest(requestId: number) {
    return latestRequestId.value === requestId;
  }

  function resetPreviewState(requestId: number) {
    if (!isLatestRequest(requestId)) {
      return;
    }
    clearPreviewState();
    loading.value = false;
  }

  function onValid(param: any, agent: number, requestId: number) {
    // 使用统一的格式化方法，将前端表单结构转换为后端 from_json 结构
    const fromJson = formatFromData(param);
    readyData(fromJson, agent, requestId);
  }
  // Methods
  const search = () => {
    const requestId = latestRequestId.value + 1;
    latestRequestId.value = requestId;
    clearPreviewState();
    loading.value = true;
    validateFormFields(
      sourceParent?.refs.formRef,
      (param: any, agent: number) => onValid(param, agent, requestId),
      () => {
        if (isLatestRequest(requestId)) {
          loading.value = false;
        }
      }
    );
  };

  const readyData = async (from: Recordable, via: number | string, requestId: number) => {
    try {
      // 获取 ticket
      if (from && (from.type === 'pspace' || from.driver === 'pspace')) {
        if (!from.params || typeof from.params !== 'object') {
          from.params = {};
        }
        // add csv_format=preview
        from.params.csv_format = 'preview';
      }
      const params: Recordable = {
        from_json: from,
        categories: categoryOpc.value
      };

      if (via) {
        params.via = via;
      }
      const result = await dataInProps.dataSource.api.fechTicketApi(params);
      if (!isLatestRequest(requestId)) {
        return;
      }
      ticket.value = result.ticket;

      // 轮询查看数据是否准备完成
      clearPollingTimer();
      timer.value = setInterval(async () => {
        if (!isLatestRequest(requestId)) {
          clearPollingTimer();
          return;
        }
        try {
          const { complete } = await dataInProps.dataSource.api.checkReadyFile(result.ticket);
          if (!isLatestRequest(requestId)) {
            return;
          }
          isComplete.value = complete;
          isShowDatasetTable.value = complete;
          if (!complete) {
            return;
          }
          clearPollingTimer();
          await getDatasetsData(requestId, result.ticket);
        } catch (error) {
          resetPreviewState(requestId);
        }
      }, 2000);
    } catch (error) {
      resetPreviewState(requestId);
    }
  };

  async function getDatasetsData(requestId: number, currentTicket: string) {
    try {
      const res = await dataInProps.dataSource.api.getDatasets(currentTicket, 1, 1000000);
      if (!isLatestRequest(requestId)) {
        return;
      }
      datasetTableData.value = res;
      isComplete.value = false;
    } catch (error) {
      resetPreviewState(requestId);
      return;
    }
    if (isLatestRequest(requestId)) {
      loading.value = false;
    }
  }

  return {
    loading,
    timer,
    isComplete,
    ticket,
    search
  };
}
