import { sendSQLReq, getPaginationData } from "@/api/explorer";

interface Params {
  currentPage: number | string;
  pageSize: number | string;
}
export function getTopics(params: Params) {
  const { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_topics;`;
  const dataSql = `select * from information_schema.ins_topics`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function getConsumers(params: Params) {
  const { currentPage, pageSize } = params;
  const countSql = `select count(*) from performance_schema.perf_consumers;`;
  const dataSql = `select cast(consumer_id as binary(100)) as consumer_id, consumer_group, client_id, status, \`topics\`, up_time, subscribe_time, rebalance_time from performance_schema.perf_consumers;`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function createTopic(sql: string) {
  return sendSQLReq(sql);
}

export function delTopic(name: string) {
  return sendSQLReq(`DROP TOPIC  \`${name}\``);
}

export function delConsumer(name: string) {
  return sendSQLReq("DROP CONSUMER GROUP " + name);
}
