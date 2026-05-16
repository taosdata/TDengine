import { isArray } from 'utils/validate';

interface usePaginationOptions<T> {
  getDataFn: AnyFunction<Promise<PaginationResult<T>>>;
  params?: Recordable;
  otherArgs?: any[];
  processDataFn?: (data: T[]) => T[];
  immediate?: boolean;
  // 分页参数是否为下划线形式
  isUnderline?: boolean;
}

export default function <T = any>({
  getDataFn,
  params = {},
  otherArgs = [],
  processDataFn = data => data,
  immediate = true,
  isUnderline = false
}: usePaginationOptions<T>) {
  const dataList = ref<T[]>([]);
  const pageSize = ref(10);
  const currentPage = ref(1);
  const total = ref(0);
  const loading = ref(false);
  const handlePageChange = async (page = 1) => {
    currentPage.value = page;
    getData();
  };
  const handleSizeChange = (size: number) => {
    pageSize.value = size;
    currentPage.value = 1;
    getData();
  };
  const getData = async () => {
    if (!getDataFn) {
      throw new Error('getDataFn is required');
    }
    if (loading.value) return;
    loading.value = true;
    const currentParams: Recordable = { ...params };
    for (const k in currentParams) {
      if (typeof currentParams[k] === 'string' && currentParams[k].trim() === '') {
        delete currentParams[k];
      }
      if (isArray(currentParams[k])) {
        if (currentParams[k].length) {
          currentParams[k] = currentParams[k].join(',');
        } else {
          delete currentParams[k];
        }
      }
    }
    if (isUnderline) {
      currentParams.page_size = pageSize.value;
      currentParams.current_page = currentPage.value;
    } else {
      currentParams.pageSize = pageSize.value;
      currentParams.currentPage = currentPage.value;
    }
    const [content, dataTotal] = await getDataFn(currentParams, ...otherArgs).catch((): PaginationResult<T> => [[], 0]);
    dataList.value = processDataFn(content);
    total.value = Number(dataTotal);
    loading.value = false;
  };
  const getDataAfterDelete = () => {
    if (loading.value) return;
    if (dataList.value.length === 1 && currentPage.value > 1) {
      currentPage.value -= 1;
    }
    getData();
  };
  if (immediate) {
    getData();
  }
  return {
    dataList,
    pageSize,
    currentPage,
    total,
    loading,
    handlePageChange,
    getData,
    getDataAfterDelete,
    handleSizeChange
  };
}
