import { sendSQLReq, getPaginationData } from "@/api/gateway/console";
export function getTopics(params) {
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_topics;`;
  const dataSql = `select * from information_schema.ins_topics`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function getConsumers(params) {
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from performance_schema.perf_consumers;`;
  const dataSql = `select cast(consumer_id as binary(100)) as consumer_id, consumer_group, client_id, status, \`topics\`, up_time, subscribe_time, rebalance_time from performance_schema.perf_consumers;`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function getSubscriptions(params) {
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_subscriptions;`;
  const dataSql = `select * from information_schema.ins_subscriptions`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function createTopic(sql) {
  return sendSQLReq(sql);
}

export function delTopic(name) {
  return sendSQLReq(`DROP TOPIC  \`${name}\``);
}

export function delConsumer(name) {
  return sendSQLReq("DROP CONSUMER GROUP " + name);
}
