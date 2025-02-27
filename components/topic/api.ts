import { getPaginationData, executeSqlFn } from '../api';
export let createTopic: RequestApiFn;
export let deleteTopic: RequestApiFn;
export let getTopicList: RequestApiFn<Recordable[]>;

export function setTopicApi(
  createTopicApi: RequestApiFn,
  deleteTopicApi: RequestApiFn,
  getTopicListApi: RequestApiFn<Recordable[]>
) {
  createTopic = createTopicApi;
  deleteTopic = deleteTopicApi;
  getTopicList = getTopicListApi;
}

export function getConsumers(params: PageQuery) {
  const { currentPage, pageSize } = params;
  const countSql = `select count(*) from performance_schema.perf_consumers;`;
  const dataSql = `select * from performance_schema.perf_consumers`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function getSubscriptions(params: PageQuery) {
  const { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_subscriptions;`;
  const dataSql = `select * from information_schema.ins_subscriptions`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function delConsumer(name: string) {
  return executeSqlFn!(`DROP CONSUMER GROUP \`${name}\``);
}
