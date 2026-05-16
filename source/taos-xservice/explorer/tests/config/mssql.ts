export const mssqlConfig = {
  connection: {
    host: '192.168.1.45',
    port: '1433',
    wrongPort: '1434', // for negative test case
    username: 'test',
    password: 'tbase125!',
  },
  dataCollection: {
    database: 'test_taosx',
    collection: 'tb_test_ci',
    sql: 'select * from test_taosx.dbo.tb_test_ci where ts > ${start} and ts < ${end};',
    start: '2024-04-16 00:00:00',
    end: '2024-04-18 00:00:00',
  },
};
