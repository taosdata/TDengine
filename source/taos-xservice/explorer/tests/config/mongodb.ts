export const mongodbConfig = {
  connection: {
    host: '192.168.1.45',
    port: '27017',
    wrongPort: '27018', // for negative test case
    username: 'admin',
    password: 'tbase125!',
  },
  dataCollection: {
    database: 'test_ci',
    collection: 'ci_7_1',
    sql: '{"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}}}',
    start: '2024-07-01 00:00:00',
    end: '2024-07-31 00:00:00',
  },
};
